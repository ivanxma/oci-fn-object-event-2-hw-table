-- Non-production target for the public employees.csv verification dataset.
CREATE TABLE IF NOT EXISTS employees_stream_verification (
  EMPLOYEE_ID BIGINT NOT NULL,
  FIRST_NAME VARCHAR(64) NOT NULL,
  LAST_NAME VARCHAR(64) NOT NULL,
  EMAIL VARCHAR(128) NOT NULL,
  PHONE_NUMBER VARCHAR(64) NULL,
  HIRE_DATE VARCHAR(32) NOT NULL,
  JOB_ID VARCHAR(32) NOT NULL,
  SALARY DECIMAL(12,2) NOT NULL,
  -- The public source uses a whitespace-padded dash as its no-commission
  -- sentinel, so preserve the source representation in this test target.
  COMMISSION_PCT VARCHAR(16) NULL,
  MANAGER_ID BIGINT NULL,
  DEPARTMENT_ID BIGINT NULL,
  batch_num BIGINT UNSIGNED NOT NULL INVISIBLE,
  PRIMARY KEY (EMPLOYEE_ID, batch_num)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY LIST (batch_num) (PARTITION p_seed VALUES IN (0));

-- Safe to re-run for the existing empty non-production verification target.
ALTER TABLE employees_stream_verification
  MODIFY COMMISSION_PCT VARCHAR(16) NULL;
