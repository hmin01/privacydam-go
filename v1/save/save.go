package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"time"

	// AWS
	"github.com/aws/aws-xray-sdk-go/xray"

	// Model
	"privacydam-go/v1/core/model"
	// Util
	"privacydam-go/v1/core/db"
)

func GenerateApi(ctx context.Context, tracking bool, api model.Api) error {
	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Generate API")
		defer subSegment.Close(nil)
	}

	// Get today
	today := time.Now()

	// Set expiration
	var expires string
	if api.ExpDate != "" {
		// Transform to integer (unit hour)
		rawExpires, err := strconv.ParseInt(api.ExpDate, 10, 64)
		if err != nil {
			return errors.New("Invalid expires format (expiration unit is hour)")
		}
		// Transform to datetime
		if rawExpires == -1 {
			expires = today.Add(time.Hour * time.Duration(2191500)).Format("2006-01-02 15:04:05")
		} else if rawExpires > 0 {
			expires = today.Add(time.Hour * time.Duration(rawExpires)).Format("2006-01-02 15:04:05")
		} else {
			return errors.New("Invalid expires (Expiration date can't be earlier than the current time)")
		}
	} else {
		return errors.New("Invalid expires (can not be blank)")
	}

	// Get database object
	dbInfo, err := db.GetDatabase("internal", nil)
	if err != nil {
		return err
	}

	// Begin transaction
	tx, err := dbInfo.Instance.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Execute query (insert API information)
	var result sql.Result
	querySyntax := `INSERT INTO api (source_id, api_name, api_alias, api_type, syntax, exp_date) VALUE (?, ?, ?, ?, ?, ?)`
	if dbInfo.Tracking {
		result, err = tx.ExecContext(subCtx, querySyntax, api.SourceId, api.Name, api.Alias, api.Type, api.QueryContent.Syntax, expires)
	} else {
		result, err = tx.Exec(querySyntax, api.SourceId, api.Name, api.Alias, api.Type, api.QueryContent.Syntax, expires)
	}
	// Catch error
	if err != nil {
		return err
	}
	// Extract inserted id
	insertedId, err := result.LastInsertId()
	if err != nil {
		return err
	}

	if len(api.QueryContent.ParamsKey) > 0 {
		// Prepare query (insert API parameters)
		var stmt *sql.Stmt
		querySyntax = `INSERT INTO parameter (api_id, parameter_key) VALUE (?, ?)`
		if dbInfo.Tracking {
			stmt, err = tx.PrepareContext(subCtx, querySyntax)
		} else {
			stmt, err = tx.Prepare(querySyntax)
		}
		// Catch error
		if err != nil {
			return err
		}

		// Execute query (insert API parameters)
		for _, param := range api.QueryContent.ParamsKey {
			var err error
			if dbInfo.Tracking {
				_, err = stmt.ExecContext(subCtx, param)
			} else {
				_, err = stmt.Exec(param)
			}
			// Catch error
			if err != nil {
				return err
			}
		}
	}

	if api.QueryContent.DidOptions != "" {
		// Execute query (insert de-identification options)
		var err error
		querySyntax := `INSERT INTO did_option (api_id, options) VALUE (?, ?)`
		if dbInfo.Tracking {
			_, err = tx.ExecContext(subCtx, querySyntax, insertedId, api.QueryContent.DidOptions)
		} else {
			_, err = tx.Exec(querySyntax, insertedId, api.QueryContent.DidOptions)
		}
		// Catch error
		if err != nil {
			return err
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	} else {
		return nil
	}
}

func DuplicateCheckForAlias(ctx context.Context, alias string) error {
	// Get database object
	dbInfo, err := db.GetDatabase("internal", nil)
	if err != nil {
		return err
	}

	// Execute query
	var result string
	querySyntax := `SELECT COUNT(*) FROM api WHERE api_alias=?`
	if dbInfo.Tracking {
		err = dbInfo.Instance.QueryRowContext(ctx, querySyntax, alias).Scan(&result)
	} else {
		err = dbInfo.Instance.QueryRow(querySyntax, alias).Scan(&result)
	}
	// Catch error
	if err != nil {
		return err
	}

	// Verify
	count, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		return err
	} else if count > int64(0) {
		return errors.New("Alias that already exist")
	} else {
		return nil
	}
}

func GenerateSource(ctx context.Context, tracking bool, source model.Source) error {
	// Get database object
	dbInfo, err := db.GetDatabase("internal", nil)
	if err != nil {
		return err
	}

	// Execute query (insert source)
	querySyntax := `INSERT INTO source (source_category, source_type, source_name, real_dsn, fake_dsn) VALUE (:source_category, :source_type, :source_name, :real_dsn, :fake_dsn)`
	if dbInfo.Tracking {
		_, err = dbInfo.Instance.NamedExecContext(ctx, querySyntax, source)
	} else {
		_, err = dbInfo.Instance.NamedExec(querySyntax, source)
	}
	// Catch error
	if err != nil {
		return err
	} else {
		// Get environment various
		isTracking, err := strconv.ParseBool(os.Getenv("IS_TRACKING"))
		if err != nil {
			return errors.New("Invalid environment various (for sql tracking)")
		}
		// Add database connection pool
		return db.CreateConnectionPool(ctx, isTracking, source, true)
	}
}
