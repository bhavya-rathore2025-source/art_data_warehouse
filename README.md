# Data Warehouse Project (Artwork Dataset)

## 📌 Overview

This project transforms raw artwork and artist data into a structured **data warehouse** using a layered architecture (**Bronze → Silver → Gold**) to enable efficient analytics.

---

## 🧱 Pipeline Summary

### 1. Bronze Layer (Raw)

* Ingested raw datasets as-is
* No transformations applied
* Stored for traceability and reprocessing

---

### 2. Silver Layer (Clean & Transform)

* Cleaned data (trim, type casting, null handling)
* Parsed `dimensions` into structured columns (`dim1`, `dim2`) using regex
* Standardized formats (dates, text)
* Removed inconsistencies and invalid records

---

### 3. Gold Layer (Analytics Ready - Star Schema)

Designed a **star schema** to support scalable queries and reporting.

---

## ⭐ Gold Schema

### 🎨 Fact Table: `gold.artworks`

Stores artwork-level information

```
object_id (PK)
title
artwork_year
date_acquired
medium
classification
department
credit_line_d
dim1
dim2
```

---

### 👨‍🎨 Dimension Table: `gold.artists`

Stores artist details

```
artist_id (PK)
display_name
artist_bio
nationality
gender
begin_date
end_date
```

---

### 🔗 Bridge Table: `gold.bridge_artwork_artist`

Handles many-to-many relationship between artworks and artists

```
object_id
artist_id
```

---

## 🔄 Key Transformations

* Converted multi-value `constituent_id` into rows using **split + explode**
* Removed redundant artist data from fact table
* Ensured proper normalization using a **bridge table**
* Replaced slow AI-based parsing with efficient regex logic

---

## 🚀 Outcome

* Clean, normalized, and scalable data model
* Fast query performance (seconds instead of minutes)
* Ready for analytics, dashboards, and business insights

---
