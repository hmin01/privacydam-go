package db

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	// Model
	"privacydam-go/v1/core/model"
	// Core (database pool)
	coreDB "privacydam-go/v1/core/db"
)

func In_findApi(ctx context.Context, param string) (model.Api, error) {
	// Create api structure
	info := model.Api{
		QueryContent: model.QueryContent{},
	}

	// Get database object
	dbInfo, err := coreDB.GetDatabase("internal", nil)
	if err != nil {
		return info, err
	}

	// Execute query (get a api information)
	var rows *sqlx.Rows
	querySyntax := `SELECT api_id, source_id, api_name, api_alias, api_type, syntax "queryContent.syntax", reg_date, exp_date, status FROM api WHERE api_alias=?`
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryxContext(ctx, querySyntax, param)
	} else {
		rows, err = dbInfo.Instance.Queryx(querySyntax, param)
	}
	// Catch error
	if err != nil {
		return info, err
	}
	defer rows.Close()

	// Extract query result
	for rows.Next() {
		if err := rows.StructScan(&info); err != nil {
			return info, err
		}
	}
	// Catch error
	if err := rows.Err(); err != nil {
		return info, err
	} else if info.Uuid == "" {
		return info, errors.New("Not found API (Please check if the API alias is correct)")
	}

	// Allocate memory to store parameters
	info.QueryContent.ParamsKey = make([]string, 0)
	// Execute query (get a list of parameters)
	querySyntax = `SELECT p.parameter_key FROM api AS a INNER JOIN parameter AS p ON a.api_id=p.api_id WHERE a.api_id=?`
	if dbInfo.Tracking {
		err = dbInfo.Instance.SelectContext(ctx, &info.QueryContent.ParamsKey, querySyntax, info.Uuid)
	} else {
		err = dbInfo.Instance.Select(&info.QueryContent.ParamsKey, querySyntax, info.Uuid)
	}
	return info, err
}

func In_getDeIdentificationOptions(ctx context.Context, id string) (string, error) {
	// Set default return value
	var options string

	// Get database object
	dbInfo, err := coreDB.GetDatabase("internal", nil)
	if err != nil {
		return options, err
	}

	// Execute query (get a de-identificaion options)
	var rows *sql.Rows
	querySyntax := `SELECT options FROM did_option WHERE api_id=?`
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryContext(ctx, querySyntax, id)
	} else {
		rows, err = dbInfo.Instance.Query(querySyntax, id)
	}
	// Catch error
	if err != nil {
		return options, err
	}
	defer rows.Close()

	// Extract query result
	for rows.Next() {
		if err := rows.Scan(&options); err != nil {
			return options, err
		}
	}

	// Return
	return options, rows.Err()
}
