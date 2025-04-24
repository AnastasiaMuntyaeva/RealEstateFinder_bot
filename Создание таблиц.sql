-- Таблица для аренды
CREATE TABLE rental (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255),
    price VARCHAR(50),
    rooms VARCHAR(20),
    area VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для покупки
CREATE TABLE sale (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255),
    property_type VARCHAR(20) CHECK (property_type IN ('вторичка', 'новостройка')),
    price VARCHAR(50),
    rooms VARCHAR(20),
    area VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
