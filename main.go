package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
)

// Define the struct to hold each row's data
type Contact struct {
	Name   string
	Mobile string
	Email  string
}

type FailedRow struct {
	Name        string
	Mobile      string
	Email       string
	ErrorReason string
	Succeed     bool
}

type FinalResponse struct {
	Status    string      `json:"status"`
	FailedRow []FailedRow `json:"failed_row"`
}

func main() {
	// Open the CSV file
	file, err := os.Open("contacts.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all the CSV records
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Slice to hold all the contacts
	var contacts []Contact

	// Iterate through the records
	for i, record := range records {
		// Skip the header row (if present)
		if i == 0 {
			continue
		}

		// Map the record to the Contact struct
		contact := Contact{
			Name:   record[0],
			Mobile: record[1],
			Email:  record[2],
		}

		// Append the contact to the slice
		contacts = append(contacts, contact)
	}

	maxWorkers := 10
	inputCh := make(chan Contact, len(contacts))
	errorCh := make(chan FailedRow, len(contacts))
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		go HandleContactMigration(inputCh, errorCh, &wg)
	}
	for i := 0; i < len(contacts); i++ {
		wg.Add(1)
		inputCh <- contacts[i]
	}
	close(inputCh)

	totalError := make([]FailedRow, 0)
	go func() {
		for failedMigration := range errorCh {
			if !failedMigration.Succeed {
				totalError = append(totalError, failedMigration)
			}
			wg.Done()
		}
	}()
	wg.Wait()
	close(errorCh)
	resp := FinalResponse{}
	if len(totalError) == 0 {
		resp.Status = "SUCCEED"
	} else if len(totalError) == len(contacts) {
		resp.Status = "FAILED"
		resp.FailedRow = totalError
	} else {
		resp.FailedRow = totalError
		resp.Status = "PARTIAL_SUCCEED"
	}
	//return response
	fmt.Println(resp)
}

func HandleContactMigration(rows <-chan Contact, errorCh chan FailedRow, wg *sync.WaitGroup) {
	for row := range rows {
		errorResp := FailedRow{
			Name:    row.Name,
			Mobile:  row.Mobile,
			Email:   row.Email,
			Succeed: false, // Default false
		}
		err := MigrateSingleRow(row)
		if err != nil {
			fmt.Println(err)
			errorResp.ErrorReason = err.Error()
			errorCh <- errorResp
			continue
		}
		errorResp.Succeed = true
		errorCh <- errorResp
	}
}

func MigrateSingleRow(row Contact) error {
	if !isValidEmail(row.Email) {
		return fmt.Errorf("Invalid email")
	}
	if !isValidMobile(row.Mobile) {
		return fmt.Errorf("Invalid mobile")
	}
	return SaveToDb(row)
}

func SaveToDb(row Contact) error {
	// Your code to save contact to your Database and return error if any
	return nil
}

func isValidMobile(phone string) bool {
	if len(phone) != 10 {
		return false
	}
	fmt.Println(string(phone[0]))
	if string(phone[0]) == "5" || string(phone[0]) == "6" || string(phone[0]) == "7" || string(phone[0]) == "8" || string(phone[0]) == "9" {
		return true
	}
	return false
}

func isValidEmail(email string) bool {
	// Define a regular expression for a valid email
	var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(email)
}

