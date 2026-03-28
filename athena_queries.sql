-- Query 1: Average billing by medical condition
SELECT medical_condition, avg_billing_amount, total_patients, avg_stay_days
FROM healthcare_db.gold_gold
WHERE medical_condition IS NOT NULL
ORDER BY avg_billing_amount DESC;

-- Query 2: Stats by hospital
SELECT hospital, total_patients, avg_billing_amount, avg_stay_days
FROM healthcare_db.gold_gold
WHERE hospital IS NOT NULL
ORDER BY total_patients DESC;

-- Query 3: Test results distribution
SELECT test_results, total_patients, avg_billing_amount
FROM healthcare_db.gold_gold
WHERE test_results IS NOT NULL
ORDER BY total_patients DESC;

-- Query 4: Admission type analysis
SELECT admission_type, total_patients, avg_billing_amount, avg_stay_days
FROM healthcare_db.gold_gold
WHERE admission_type IS NOT NULL
ORDER BY total_patients DESC;