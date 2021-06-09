package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	// AWS
	"github.com/aws/aws-xray-sdk-go/xray"

	// Util
	"privacydam-go/v1/core/db"
)

/*
 * Initialize database (create database connection pool)
 * <IN> ctx (context.Context): context
 * <OUT> (error): error object (contain nil)
 */
func InitializeDatabase(ctx context.Context, configPath string) error {
	// [For debug] set subsegment
	subCtx, subSegment := xray.BeginSegment(ctx, "Initialize database")
	defer subSegment.Close(nil)

	// Load configuration and set environment various
	if err := loadConfiguration(configPath); err != nil {
		return err
	}

	// Initialize database
	return db.Initialization(subCtx)
}

/*
 * Load configuration (contain generate DSN)
 * <IN> configPath (string): database configuration file path
 * <OUT> (error): error object (contain nil)
 */
func loadConfiguration(configPath string) error {
	// Load a database configuration
	rawConfiguration, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	// Transform to map
	var config map[string]string
	if err := json.Unmarshal(rawConfiguration, &config); err != nil {
		return err
	}

	// Generate DSN
	var dsn string
	switch config["name"] {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config["username"], config["password"], config["host"], config["port"], config["database"])
	case "hdb":
		dsn = fmt.Sprintf("hdb://%s:%s@%s:%s", config["username"], config["password"], config["host"], config["port"])
	}

	// Return DSN
	if dsn == "" {
		return errors.New("DSN creation failed.")
	} else {
		// Set environment various
		os.Setenv("DSN", dsn)
		os.Setenv("IS_TRACKING", config["tracking"])
		return nil
	}
}

// /*
//  * Add database connection pool
//  * <IN> ctx (context.Context): context
//  * <IN> tracking (bool): tracking with AWS X-Ray
//  * <IN> source (model.Source): source information object
//  * <OUT> (error): error object (contain nil)
//  */
// func AddDatabaseConnectionPool(ctx context.Context, tracking bool, source model.Source) error {
// 	return db.CreateConnectionPool(ctx, tracking, source, true)
// }
