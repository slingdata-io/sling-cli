
-- Drop temp tables if they exist
IF OBJECT_ID('dbo.unique_identifier_test', 'U') IS NOT NULL DROP TABLE dbo.unique_identifier_test;
IF OBJECT_ID('dbo.unique_identifier_test2', 'U') IS NOT NULL DROP TABLE dbo.unique_identifier_test2;

-- Create source table with uniqueidentifier column
CREATE TABLE dbo.unique_identifier_test (
  id INT PRIMARY KEY,
  guid_col UNIQUEIDENTIFIER DEFAULT NEWID(),
  name VARCHAR(100),
  created_at DATETIME DEFAULT GETDATE()
);

-- Insert test data with specific GUIDs
INSERT INTO dbo.unique_identifier_test (id, guid_col, name) VALUES 
(1, '6F9619FF-8B86-D011-B42D-00C04FC964FF', 'Test Record 1'),
(2, 'A0E44FF6-8B86-D011-B42D-00C04FC964FF', 'Test Record 2'),
(3, NEWID(), 'Test Record 3'),
(4, NULL, 'Test Record 4 with NULL GUID'),
(5, '12345678-1234-5678-1234-567812345678', 'Test Record 5');