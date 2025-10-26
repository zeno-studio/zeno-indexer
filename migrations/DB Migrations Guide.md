# Database Migrations Guide

## 📋 Overview

This directory contains PostgreSQL database migration files for the zeno-indexer project. We use [SQLx](https://github.com/launchbadge/sqlx) for compile-time verified queries and migrations.

---

## 🏗️ Migration File Naming Convention

```
YYYYMMDD_description.sql
```

**Examples:**
- `20250818_create_tables.sql` - Initial schema
- `20250820_add_price_column.sql` - Add new column
- `20250825_create_user_table.sql` - Add new table

**Rules:**
- ✅ Use date prefix (YYYYMMDD) for chronological ordering
- ✅ Use descriptive names with underscores
- ✅ Use lowercase letters
- ✅ Keep names concise but clear

---

## 📝 Creating New Migrations

### 1️⃣ Adding a New Table

**File:** `migrations/YYYYMMDD_create_new_table.sql`

```sql
-- ============================================
-- Migration: Create [table_name] table
-- Date: YYYY-MM-DD
-- Description: [Purpose of this table]
-- ============================================

CREATE TABLE IF NOT EXISTS table_name (
    id SERIAL PRIMARY KEY,
    
    -- Required fields
    field1 TEXT NOT NULL,
    field2 BIGINT NOT NULL,
    
    -- Optional fields
    field3 TEXT,
    field4 JSONB,
    
    -- Timestamps (recommended for all tables)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    
    -- Constraints
    UNIQUE(field1, field2)
);

-- Indexes for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_table_name_field1 ON table_name(field1);
CREATE INDEX IF NOT EXISTS idx_table_name_created_at ON table_name(created_at DESC);

-- Comments for documentation
COMMENT ON TABLE table_name IS 'Description of what this table stores';
COMMENT ON COLUMN table_name.field1 IS 'Description of field1';
```

**Checklist:**
- [ ] Use `IF NOT EXISTS` for idempotency
- [ ] Add primary key (usually `id SERIAL PRIMARY KEY`)
- [ ] Add `created_at` and `updated_at` timestamps
- [ ] Define `NOT NULL` constraints appropriately
- [ ] Add UNIQUE constraints where needed
- [ ] Create indexes for query optimization
- [ ] Add table and column comments

---

### 2️⃣ Adding New Columns to Existing Table

**File:** `migrations/YYYYMMDD_add_column_to_table.sql`

```sql
-- ============================================
-- Migration: Add [column_name] to [table_name]
-- Date: YYYY-MM-DD
-- Description: [Why this column is needed]
-- ============================================

-- Add new column (nullable first for safety)
ALTER TABLE table_name 
ADD COLUMN IF NOT EXISTS new_column TEXT;

-- Optional: Set default value for existing rows
UPDATE table_name 
SET new_column = 'default_value' 
WHERE new_column IS NULL;

-- Optional: Make column NOT NULL after populating
ALTER TABLE table_name 
ALTER COLUMN new_column SET NOT NULL;

-- Add index if needed
CREATE INDEX IF NOT EXISTS idx_table_name_new_column 
ON table_name(new_column);

-- Add comment
COMMENT ON COLUMN table_name.new_column IS 'Description of the new column';
```

**Important:**
- ⚠️ **Always add columns as nullable first** to avoid breaking existing data
- ⚠️ **Backfill data** before adding NOT NULL constraint
- ⚠️ **Test rollback** in development environment first

---

### 3️⃣ Modifying Existing Columns

**File:** `migrations/YYYYMMDD_modify_column.sql`

```sql
-- ============================================
-- Migration: Modify [column_name] in [table_name]
-- Date: YYYY-MM-DD
-- Description: [What is being changed and why]
-- ============================================

-- Change column type
ALTER TABLE table_name 
ALTER COLUMN column_name TYPE NEW_TYPE USING column_name::NEW_TYPE;

-- Add/remove NOT NULL constraint
ALTER TABLE table_name 
ALTER COLUMN column_name SET NOT NULL;  -- or DROP NOT NULL

-- Add default value
ALTER TABLE table_name 
ALTER COLUMN column_name SET DEFAULT 'value';

-- Rename column (use with caution!)
ALTER TABLE table_name 
RENAME COLUMN old_name TO new_name;
```

**⚠️ Breaking Changes Warning:**
- Changing column types may break existing code
- Renaming columns will break all queries using old name
- Always update Rust code simultaneously

---

### 4️⃣ Adding Indexes

**File:** `migrations/YYYYMMDD_add_indexes.sql`

```sql
-- ============================================
-- Migration: Add indexes for performance optimization
-- Date: YYYY-MM-DD
-- Description: [Query patterns being optimized]
-- ============================================

-- Standard B-tree index (default)
CREATE INDEX IF NOT EXISTS idx_table_column 
ON table_name(column_name);

-- Composite index (order matters!)
CREATE INDEX IF NOT EXISTS idx_table_col1_col2 
ON table_name(column1, column2);

-- Partial index (filtered)
CREATE INDEX IF NOT EXISTS idx_table_active_users 
ON table_name(status) 
WHERE status = 'active';

-- Index for JSON fields (GIN index)
CREATE INDEX IF NOT EXISTS idx_table_json_field 
ON table_name USING GIN (json_column);

-- Descending index (for ORDER BY DESC queries)
CREATE INDEX IF NOT EXISTS idx_table_created_desc 
ON table_name(created_at DESC);
```

**Index Guidelines:**
- ✅ Create indexes on foreign key columns
- ✅ Create indexes on columns used in WHERE clauses
- ✅ Create indexes on columns used in ORDER BY
- ✅ Create indexes on columns used in JOIN conditions
- ❌ Don't over-index (slows down writes)
- ❌ Don't index small tables (< 1000 rows)

---

## 🎯 Best Practices

### 1. **Idempotency** (可重复执行性)
```sql
-- ✅ Good - Can run multiple times safely
CREATE TABLE IF NOT EXISTS users (...);
ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;

-- ❌ Bad - Will fail on second run
CREATE TABLE users (...);
ALTER TABLE users ADD COLUMN email TEXT;
```

### 2. **Transaction Safety**
```sql
-- For complex migrations, use transactions
BEGIN;

ALTER TABLE users ADD COLUMN status TEXT;
UPDATE users SET status = 'active';
ALTER TABLE users ALTER COLUMN status SET NOT NULL;

COMMIT;
```

### 3. **Data Type Selection**

| Use Case | Data Type | Notes |
|----------|-----------|-------|
| Ethereum addresses | `TEXT` | Store as lowercase hex string |
| Chain IDs | `BIGINT` | Some chains use large numbers |
| Token amounts | `NUMERIC(78, 0)` | For precise uint256 values |
| USD prices | `DOUBLE PRECISION` | Sufficient for price data |
| Timestamps | `TIMESTAMP` | Use `CURRENT_TIMESTAMP` default |
| JSON data | `JSONB` | Use JSONB (not JSON) for indexing |
| Boolean flags | `BOOLEAN` | Don't use integers (0/1) |
| Enums | `TEXT` | PostgreSQL ENUMs are hard to change |

### 4. **Naming Conventions**

```sql
-- Tables: lowercase, plural
CREATE TABLE users;
CREATE TABLE token_prices;

-- Columns: lowercase, snake_case
column_name TEXT
created_at TIMESTAMP

-- Indexes: idx_tablename_columnname
idx_users_email
idx_token_prices_timestamp

-- Constraints: tablename_columnname_key/fkey
users_email_key
orders_user_id_fkey
```

### 5. **Foreign Keys**

```sql
-- Add foreign key with proper naming
ALTER TABLE metadata
ADD CONSTRAINT metadata_chainid_fkey 
FOREIGN KEY (chainid) 
REFERENCES chains(chainid) 
ON DELETE CASCADE;  -- or RESTRICT, SET NULL

-- Create index on foreign key column
CREATE INDEX idx_metadata_chainid ON metadata(chainid);
```

### 6. **Performance Considerations**

```sql
-- ⚠️ Adding NOT NULL to large table locks it
-- Better approach for large tables:
ALTER TABLE large_table ADD COLUMN new_col TEXT;  -- Nullable first
UPDATE large_table SET new_col = 'value' WHERE new_col IS NULL;  -- In batches
ALTER TABLE large_table ALTER COLUMN new_col SET NOT NULL;  -- After all data populated

-- ⚠️ Adding indexes to large tables is slow
-- Consider using CONCURRENTLY (doesn't lock table)
CREATE INDEX CONCURRENTLY idx_large_table_col ON large_table(column_name);
```

---

## 🔄 Migration Workflow

### Development Environment

```bash
# 1. Create new migration file
touch migrations/$(date +%Y%m%d)_description.sql

# 2. Edit the migration file
vim migrations/YYYYMMDD_description.sql

# 3. Test migration
sqlx migrate run

# 4. Verify changes
psql $DATABASE_URL -c "\d table_name"

# 5. Test rollback (if needed)
sqlx migrate revert

# 6. Commit migration file
git add migrations/YYYYMMDD_description.sql
git commit -m "feat: add migration for [description]"
```

### Production Deployment

```bash
# 1. Backup database first!
pg_dump $DATABASE_URL > backup_$(date +%Y%m%d).sql

# 2. Test in staging environment
sqlx migrate run --database-url $STAGING_DATABASE_URL

# 3. Deploy to production
sqlx migrate run --database-url $PRODUCTION_DATABASE_URL

# 4. Verify migration
sqlx migrate info
```

---

## ⚠️ Common Pitfalls

### 1. **Breaking Changes**
```sql
-- ❌ DON'T: Remove columns still used by application
ALTER TABLE users DROP COLUMN email;

-- ✅ DO: Deprecate first, remove later
-- Step 1 (Deploy): Stop using column in code
-- Step 2 (Wait 1 week): Verify no usage
-- Step 3 (Deploy): Remove column in migration
```

### 2. **Large Table Migrations**
```sql
-- ❌ DON'T: Update millions of rows at once
UPDATE large_table SET status = 'active';

-- ✅ DO: Batch updates
DO $$
DECLARE
    batch_size INT := 1000;
BEGIN
    LOOP
        UPDATE large_table 
        SET status = 'active'
        WHERE id IN (
            SELECT id FROM large_table 
            WHERE status IS NULL 
            LIMIT batch_size
        );
        
        EXIT WHEN NOT FOUND;
        COMMIT;
    END LOOP;
END $$;
```

### 3. **Missing Indexes**
```sql
-- ❌ Forgetting indexes on UNIQUE constraints
ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email);
-- PostgreSQL creates index automatically ✅

-- ❌ Forgetting indexes on foreign keys
ALTER TABLE orders ADD COLUMN user_id BIGINT REFERENCES users(id);
-- No automatic index! Must create manually:
CREATE INDEX idx_orders_user_id ON orders(user_id);
```

---

## 📚 SQLx-Specific Notes

### Compile-Time Verification

```rust
// SQLx will verify this query at compile time
let users = sqlx::query_as::<_, User>(
    "SELECT id, name, email FROM users WHERE status = $1"
)
.bind("active")
.fetch_all(&pool)
.await?;
```

**Important:**
- After adding migrations, run: `cargo sqlx prepare`
- This creates `.sqlx/` directory with query metadata
- Commit `.sqlx/` files to version control

### Migration Macros

```rust
// In main.rs or config.rs
sqlx::migrate!("./migrations")
    .run(&pool)
    .await
    .expect("Failed to run migrations");
```

---

## 🔍 Verification Checklist

Before committing a migration:

- [ ] Migration file follows naming convention
- [ ] Uses `IF NOT EXISTS` for idempotency
- [ ] Tested in local development database
- [ ] Tested migration and rollback (if applicable)
- [ ] Updated Rust structs to match schema changes
- [ ] Run `cargo sqlx prepare` if queries changed
- [ ] Added comments explaining the change
- [ ] Verified indexes are appropriate
- [ ] Considered performance impact on large tables
- [ ] Documented any breaking changes

---

## 📖 References

- [SQLx Documentation](https://github.com/launchbadge/sqlx)
- [PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html)
- [PostgreSQL Indexes](https://www.postgresql.org/docs/current/indexes.html)
- [PostgreSQL Data Types](https://www.postgresql.org/docs/current/datatype.html)

---

## 🆘 Troubleshooting

### Migration Failed

```bash
# Check migration status
sqlx migrate info

# Force specific version
sqlx migrate run --target-version 20250818

# Manually fix and retry
psql $DATABASE_URL
# ... fix issue manually
sqlx migrate run
```

### Rollback Migration

```bash
# SQLx doesn't support automatic rollback
# Manual rollback required

# Option 1: Restore from backup
pg_restore -d database_name backup_file.sql

# Option 2: Write reverse migration
# Create new migration that undoes previous changes
```

---

**Last Updated:** 2025-10-26  
**Maintainer:** Zeno Indexer Team
