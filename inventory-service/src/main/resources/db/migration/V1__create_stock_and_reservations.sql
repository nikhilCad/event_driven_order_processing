CREATE TABLE stock (
    product_id VARCHAR(64) PRIMARY KEY,
    available  INTEGER NOT NULL CHECK (available >= 0)
);

CREATE TABLE reservations (
    id         UUID PRIMARY KEY,
    order_id   UUID         NOT NULL,
    product_id VARCHAR(64)  NOT NULL,
    quantity   INTEGER      NOT NULL CHECK (quantity > 0),
    status     VARCHAR(32)  NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_reservations_order_id ON reservations (order_id);
