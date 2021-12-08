CREATE TABLE transactions(
    idx SERIAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    source CHAR(40) NOT NULL,
    target CHAR(40) NOT NULL,
    amount DECIMAL(65) UNSIGNED NOT NULL,
    INDEX(source),
    INDEX(target));

CREATE TABLE ether_transactions(
    remote_transaction CHAR(64) NOT NULL,
    local_transaction BIGINT UNSIGNED NOT NULL,
    INDEX(remote_transaction),
    FOREIGN KEY(local_transaction) REFERENCES transactions(idx));

CREATE TABLE deposit_scans(
    idx SERIAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    start_block BIGINT UNSIGNED NOT NULL,
    end_block BIGINT UNSIGNED NOT NULL,
    transactions JSON,
    INDEX(end_block));
