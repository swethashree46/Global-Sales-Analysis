CREATE OR REPLACE TABLE project-p2-460512.final_dataset.product_summary AS
SELECT
  MIN(SaleId) AS SaleId,
  Product,
  Category,
  Year,
  SUM(Qty) AS Qty,
  AVG(Sale) AS Sale,
  SUM(Amount) AS Amount
FROM project-p2-460512.final_dataset.final_sales
GROUP BY Product, Category, Year;

CREATE OR REPLACE TABLE project-p2-460512.final_dataset.tax_report AS
SELECT
  Country,
  Product,
  Category,
  Year,
  SUM(Qty) AS Qty,
  SUM(Amount) AS Amount,
  ROUND(SUM(Amount) * 0.05, 2) AS Tax
FROM project-p2-460512.final_dataset.final_sales
GROUP BY Country, Product, Category, Year;

 CREATE OR REPLACE MODEL project-p2-460512.final_dataset.inr_model
OPTIONS (
  model_type = 'linear_reg',
  input_label_cols = ['INR_Amount']
) AS
SELECT
  Qty,
  Sale,
  Country,
  Category,
  Year,
  INR_Amount
FROM project-p2-460512.final_dataset.final_sales;

CREATE OR REPLACE TABLE project-p2-460512.final_dataset.inr_predictions AS
SELECT
  input_data.*,
  GREATEST(prediction.predicted_INR_Amount, 0) AS predicted_INR_Amount
FROM
  ML.PREDICT(
    MODEL project-p2-460512.final_dataset.inr_model,
    (
      SELECT Qty, Sale, Country, Category, Year
      FROM project-p2-460512.final_dataset.final_sales
    )
  ) AS prediction
JOIN project-p2-460512.final_dataset.final_sales AS input_data
  ON input_data.Qty = prediction.Qty
  AND input_data.Sale = prediction.Sale
  AND input_data.Country = prediction.Country
  AND input_data.Category = prediction.Category
  AND input_data.Year = prediction.Year;


