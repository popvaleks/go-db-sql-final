package main

import (
	"database/sql"
	"errors"
	_ "modernc.org/sqlite"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("address", p.Address),
		sql.Named("status", p.Status),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("SELECT client, status, address, created_at, number FROM parcel WHERE number = :number", sql.Named("number", number))

	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Client, &p.Status, &p.Address, &p.CreatedAt, &p.Number)
	if err != nil {
		return p, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	rows, err := s.db.Query("SELECT client, status, address, created_at, number FROM parcel WHERE client = :client", sql.Named("client", client))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// заполните срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		p := Parcel{}
		err := rows.Scan(&p.Client, &p.Status, &p.Address, &p.CreatedAt, &p.Number)
		if err != nil {
			return nil, err
		}

		res = append(res, p)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel

	return nil
}

func (s ParcelStore) isRegistered(number int) (bool, error) {
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number", sql.Named("number", number))

	var status string

	err := row.Scan(&status)
	if err != nil {
		return false, err
	}

	if status != ParcelStatusRegistered {
		return false, errors.New("parcel status must be registered")
	}

	return true, nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	_, err := s.isRegistered(number)
	if err != nil {
		return err
	}

	_, err = s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number",
		sql.Named("number", number),
		sql.Named("address", address),
	)
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	_, err := s.isRegistered(number)
	if err != nil {
		return err
	}
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	_, err = s.db.Exec("DELETE from parcel WHERE number = :number", sql.Named("number", number))
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	if err != nil {
		return err
	}

	return nil
}
