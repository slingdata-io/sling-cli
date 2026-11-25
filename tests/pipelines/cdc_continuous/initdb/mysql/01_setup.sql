GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sling_repl'@'%';
GRANT SELECT ON *.* TO 'sling_repl'@'%';
FLUSH PRIVILEGES;

CREATE TABLE cdc_cont_db.orders (id INT PRIMARY KEY, note VARCHAR(50));
CREATE TABLE cdc_cont_db.items (id INT PRIMARY KEY, note VARCHAR(50));
INSERT INTO cdc_cont_db.orders VALUES (1, 'o1'), (2, 'o2'), (3, 'o3');
INSERT INTO cdc_cont_db.items VALUES (1, 'i1'), (2, 'i2');
