-- Tabla de clientes
CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    createDate DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Tabla de préstamos con status
CREATE TABLE IF NOT EXISTS loans (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL REFERENCES clients(id),
    original_amount NUMERIC(12,2) NOT NULL,
    current_balance NUMERIC(12,2) NOT NULL,
    granting_date DATE NOT NULL,
    created_date DATE NOT NULL DEFAULT CURRENT_DATE,
    interest_rate NUMERIC(5,2) NOT NULL,
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    folio VARCHAR(20) UNIQUE,
    status VARCHAR(15) NOT NULL DEFAULT 'active' CHECK (
        status IN ('active', 'closed', 'defaulted', 'cancelled')
    )
);

-- Índices para loans
CREATE INDEX IF NOT EXISTS idx_loans_client_id ON loans(client_id);
CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status);
CREATE INDEX IF NOT EXISTS idx_loans_folio ON loans(folio);

-- Tabla de movimientos
CREATE TABLE IF NOT EXISTS movements (
    id SERIAL PRIMARY KEY,
    loan_id INTEGER NOT NULL REFERENCES loans(id),
    movement_type VARCHAR(20) NOT NULL CHECK (
        movement_type IN (
            'interest_payment',
            'principal_payment',
            'interest_charge',
            'late_fee_charge',
            'adjustment'
        )
    ),
    amount NUMERIC(12,2) NOT NULL,
    previous_balance NUMERIC(12,2) NOT NULL,
    new_balance NUMERIC(12,2) NOT NULL,
    movement_date DATE NOT NULL DEFAULT CURRENT_DATE,
    application_period VARCHAR(20),
    reference VARCHAR(50),
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para movements
CREATE INDEX IF NOT EXISTS idx_movements_loan_id ON movements(loan_id);
CREATE INDEX IF NOT EXISTS idx_movements_type ON movements(movement_type);
CREATE INDEX IF NOT EXISTS idx_movements_date ON movements(movement_date);
CREATE INDEX IF NOT EXISTS idx_movements_period ON movements(application_period);

-- Tabla de estados de cuenta
CREATE TABLE IF NOT EXISTS statements (
    id SERIAL PRIMARY KEY,
    loan_id INTEGER NOT NULL REFERENCES loans(id),
    period VARCHAR(20) NOT NULL,
    initial_balance NUMERIC(12,2) NOT NULL,
    final_balance NUMERIC(12,2) NOT NULL,
    interest_generated NUMERIC(12,2) NOT NULL,
    interest_paid NUMERIC(12,2) DEFAULT 0.00,
    principal_paid NUMERIC(12,2) DEFAULT 0.00,
    late_fee_generated NUMERIC(12,2) DEFAULT 0.00,
    cut_off_date DATE NOT NULL,
    due_date DATE NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'paid', 'overdue', 'partial')
    ),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para statements
CREATE INDEX IF NOT EXISTS idx_statements_loan_id ON statements(loan_id);
CREATE INDEX IF NOT EXISTS idx_statements_period ON statements(period);
CREATE INDEX IF NOT EXISTS idx_statements_status ON statements(status);
CREATE INDEX IF NOT EXISTS idx_statements_due_date ON statements(due_date);

-- Índice único para evitar duplicados de periodo por préstamo
CREATE UNIQUE INDEX IF NOT EXISTS ux_statements_loan_period ON statements(loan_id, period);

-- Tabla de configuración de tasas (opcional para futura escalabilidad)
CREATE TABLE IF NOT EXISTS rate_configuration (
    id SERIAL PRIMARY KEY,
    loan_type VARCHAR(50) NOT NULL,
    min_amount NUMERIC(12,2),
    max_amount NUMERIC(12,2),
    interest_rate NUMERIC(5,2) NOT NULL,
    term_months INTEGER,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para rate_configuration
CREATE INDEX IF NOT EXISTS idx_rate_config_type ON rate_configuration(loan_type);
CREATE INDEX IF NOT EXISTS idx_rate_config_active ON rate_configuration(active);
CREATE INDEX IF NOT EXISTS idx_rate_config_dates ON rate_configuration(effective_date, expiration_date);

-- Insertar configuración de tasas por defecto (si no existe)
INSERT INTO rate_configuration (loan_type, interest_rate, effective_date, active) 
VALUES ('interest_on_balance', 10.00, CURRENT_DATE, TRUE)
ON CONFLICT DO NOTHING;