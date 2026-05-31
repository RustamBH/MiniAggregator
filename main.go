package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

type UserData struct {
	ProfileData string `json:"profile,omitempty"`
	OrdersData  string `json:"orders,omitempty"`
	RecsData    string `json:"recs,omitempty"`
	Error       error  `json:"error,omitempty"`
}

func profileHandler(w http.ResponseWriter, r *http.Request) {
	// Случайная задержка 500–800 ms
	delay := time.Duration(500+rand.Intn(301)) * time.Millisecond
	time.Sleep(delay)

	// 20 % вероятность ошибки
	if rand.Intn(100) < 20 {
		http.Error(w, "profile service error", http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, `{"id":1,"name":"Alice","email":"alice@example.com"}`)
}

func ordersHandler(w http.ResponseWriter, r *http.Request) {
	delay := time.Duration(500+rand.Intn(301)) * time.Millisecond
	time.Sleep(delay)

	// 15 % вероятность паники (имитация)
	if rand.Intn(100) < 15 {
		panic("orders service panicked")
	}

	fmt.Fprintln(w, `{"orders":[{"id":1,"amount":100},{"id":2,"amount":200}]}`)
}

func recsHandler(w http.ResponseWriter, r *http.Request) {
	delay := time.Duration(500+rand.Intn(301)) * time.Millisecond
	time.Sleep(delay)

	// 10 % вероятность ошибки
	if rand.Intn(100) < 10 {
		http.Error(w, "recommendations service error", http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, `{"recommendations":["book1","book2","book3"]}`)
}

// startMockServer запускает локальный тестовый HTTP-сервер с тремя эндпоинтами.
// /profile и /orders, /recommendations,
// чтобы продемонстрировать срабатывание таймаута контекста.
func startMockServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/profile", profileHandler)
	mux.HandleFunc("/orders", ordersHandler)
	mux.HandleFunc("/recommendations", recsHandler)

	return httptest.NewServer(mux)
}

// safeFetch — обёртка для защиты от паник и обработки ошибок
func safeFetch(ctx context.Context, client *http.Client, url string) (string, error) {
	//ch := make(chan interface{}, 1)
	ch := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic occurred in %s: %v", url, r)
			}
		}()

		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			errCh <- fmt.Errorf("HTTP error %d from %s", resp.StatusCode, url)
			return
		}

		var data string
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			errCh <- err
			return
		}
		ch <- data
	}()

	select {
	case data := <-ch:
		return data, nil
	case err := <-errCh:
		return "", err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// fetchService — воркер, который делает один запрос и отправляет результат в общий канал
func fetchService(
	ctx context.Context,
	client *http.Client,
	url string,
	serviceName string,
	dataCh chan<- *UserData,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	data, err := safeFetch(ctx, client, url)

	result := &UserData{}
	switch serviceName {
	case "profile":
		result.ProfileData = data
	case "orders":
		result.OrdersData = data
	case "recommendations":
		result.RecsData = data
	}
	if err != nil {
		result.Error = err
	}

	dataCh <- result
}

func main() {
	server := startMockServer()
	defer server.Close()

	client := &http.Client{Timeout: 1 * time.Second}
	services := []struct {
		url         string
		serviceName string
	}{
		{server.URL + "/profile", "profile"},
		{server.URL + "/orders", "orders"},
		{server.URL + "/recommendations", "recommendations"},
	}

	// 1. Создаем контекст, который будет отменен по сигналу ОС
	//ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

	time.AfterFunc(3*time.Second, func() {
		fmt.Println("\nПолучен сигнал SIGTERM. Запуск принудительной остановки graceful shutdown...")
		defer cancel() // Освобождаем ресурсы при выходе
	})

	// Fan‑out: запускаем горутины для каждого сервиса
	var wg sync.WaitGroup
	dataCh := make(chan *UserData, len(services)) // Fan‑out: запускаем горутины для каждого сервиса

	for _, s := range services {
		wg.Add(1)
		go fetchService(ctx, client, s.url, s.serviceName, dataCh, &wg)
	}

	// 3. Блокируем main до получения сигнала остановки
	<-ctx.Done()
	log.Println("\nПолучен сигнал остановки (SIGINT/SIGTERM). Ожидаем завершения воркеров...")

	// 4. Устанавливаем таймаут для graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Используем канал, чтобы дождаться, либо завершения воркеров, либо таймаута
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(dataCh)
		close(done)
	}()

	select {
	case <-done:
		log.Println("Все воркеры завершили работу. Сервис останавливается.")
	case <-shutdownCtx.Done():
		log.Println("Таймаут graceful shutdown завершен. Принудительная остановка.")
	}

	// Fan-In: собираем результаты в общую структуру
	finalRes := &UserData{}

	for partialRes := range dataCh {
		if partialRes.Error != nil && finalRes.Error == nil {
			finalRes.Error = partialRes.Error
		}

		if partialRes.ProfileData != "" {
			finalRes.ProfileData = partialRes.ProfileData
		}

		if partialRes.OrdersData != "" {
			finalRes.OrdersData = partialRes.OrdersData
		}

		if partialRes.RecsData != "" {
			finalRes.RecsData = partialRes.RecsData
		}
	}

	// Выводим итоговый результат
	fmt.Println("\n=== ИТОГОВЫЙ РЕЗУЛЬТАТ ===")
	output, _ := json.MarshalIndent(finalRes, "", "  ")
	fmt.Println(string(output))
	fmt.Println("Операция завершена. Причина:", ctx.Err())
}
