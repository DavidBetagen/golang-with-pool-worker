package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/panjf2000/ants/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB
var wg sync.WaitGroup
var pool *ants.PoolWithFunc

// Student struct
type Student struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"size:100;not null"`
	Email string `gorm:"size:100;unique;not null"`
}

// Insert student data into the database
func insertStudent(s Student) {
	if err := db.Create(&s).Error; err != nil {
		log.Printf("Error inserting student: %v", err)
	} else {
		log.Printf("Inserted student: %s", s.Name)
	}
}

func main() {
	var err error
	// Connect to PostgreSQL
	dsn := "user=youruser password=yourpass dbname=yourdb sslmode=disable"
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Auto Migrate
	db.AutoMigrate(&Student{})

	// Create a worker pool
	pool, _ = ants.NewPoolWithFunc(10, func(i interface{}) {
		insertStudent(i.(Student))
		wg.Done()
	})

	// Fiber app
	app := fiber.New()

	app.Post("/add-student", func(c *fiber.Ctx) error {
		student := new(Student)
		if err := c.BodyParser(student); err != nil {
			return c.Status(400).SendString("Invalid input")
		}
		wg.Add(1)
		_ = pool.Invoke(*student)
		return c.SendString("Student added to queue")
	})

	// Graceful shutdown handling
	go func() {
		if err := app.Listen(":3000"); err != nil {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	pool.Release()
	wg.Wait() // Wait for all jobs to complete before exiting
	log.Println("Server shutdown complete.")
}
