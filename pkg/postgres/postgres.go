package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"ssl_mode"`

	MaxConns int32 `yaml:"max_conns"`
	MinConns int32 `yaml:"min_conns"`
}

type Postgres struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, cfg Config) (*Postgres, error) {
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.SSLMode,
	)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}

	poolCfg.MaxConnLifetime = 1 * time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute
	poolCfg.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return &Postgres{
		pool: pool,
	}, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}

func (p *Postgres) Pool() *pgxpool.Pool {
	return p.pool
}
