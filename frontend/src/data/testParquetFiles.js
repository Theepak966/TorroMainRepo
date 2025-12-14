// Hardcoded test parquet files for each data type

export const TEST_PARQUET_FILES = {
  sample: [
    { id: 'sample_1', name: 'sample_data_1.parquet', folder: 'test_data/sample', rows: 1000, size: 245760, columns: ['id', 'name', 'value', 'timestamp', 'category'] },
    { id: 'sample_2', name: 'sample_data_2.parquet', folder: 'test_data/sample', rows: 1500, size: 367680, columns: ['id', 'name', 'value', 'timestamp', 'category', 'status'] },
    { id: 'sample_3', name: 'sample_data_3.parquet', folder: 'test_data/sample', rows: 2000, size: 490560, columns: ['id', 'name', 'value', 'timestamp', 'category', 'status', 'priority'] },
    { id: 'sample_4', name: 'sample_data_4.parquet', folder: 'test_data/sample', rows: 800, size: 196608, columns: ['id', 'name', 'value', 'timestamp'] },
    { id: 'sample_5', name: 'sample_data_5.parquet', folder: 'test_data/sample', rows: 1200, size: 294912, columns: ['id', 'name', 'value', 'timestamp', 'category', 'tags'] },
  ],
  sales: [
    { id: 'sales_1', name: 'sales_q1_2024.parquet', folder: 'test_data/sales', rows: 5000, size: 1228800, columns: ['sale_id', 'product_id', 'customer_id', 'amount', 'date', 'region', 'salesperson'] },
    { id: 'sales_2', name: 'sales_q2_2024.parquet', folder: 'test_data/sales', rows: 5200, size: 1277952, columns: ['sale_id', 'product_id', 'customer_id', 'amount', 'date', 'region', 'salesperson', 'discount'] },
    { id: 'sales_3', name: 'sales_q3_2024.parquet', folder: 'test_data/sales', rows: 4800, size: 1179648, columns: ['sale_id', 'product_id', 'customer_id', 'amount', 'date', 'region', 'salesperson', 'discount', 'tax'] },
    { id: 'sales_4', name: 'sales_q4_2024.parquet', folder: 'test_data/sales', rows: 5500, size: 1351680, columns: ['sale_id', 'product_id', 'customer_id', 'amount', 'date', 'region', 'salesperson', 'discount', 'tax', 'total'] },
    { id: 'sales_5', name: 'sales_annual_2024.parquet', folder: 'test_data/sales', rows: 20500, size: 5038080, columns: ['sale_id', 'product_id', 'customer_id', 'amount', 'date', 'region', 'salesperson', 'discount', 'tax', 'total', 'payment_method'] },
  ],
  users: [
    { id: 'users_1', name: 'users_active.parquet', folder: 'test_data/users', rows: 10000, size: 2457600, columns: ['user_id', 'username', 'email', 'created_at', 'last_login', 'status', 'role'] },
    { id: 'users_2', name: 'users_inactive.parquet', folder: 'test_data/users', rows: 3000, size: 737280, columns: ['user_id', 'username', 'email', 'created_at', 'last_login', 'status', 'inactive_date'] },
    { id: 'users_3', name: 'users_new.parquet', folder: 'test_data/users', rows: 1500, size: 368640, columns: ['user_id', 'username', 'email', 'created_at', 'registration_source'] },
    { id: 'users_4', name: 'users_premium.parquet', folder: 'test_data/users', rows: 2500, size: 614400, columns: ['user_id', 'username', 'email', 'subscription_type', 'subscription_start', 'subscription_end'] },
    { id: 'users_5', name: 'users_metadata.parquet', folder: 'test_data/users', rows: 8000, size: 1966080, columns: ['user_id', 'username', 'email', 'profile_data', 'preferences', 'metadata'] },
  ],
  products: [
    { id: 'products_1', name: 'products_catalog.parquet', folder: 'test_data/products', rows: 5000, size: 1228800, columns: ['product_id', 'name', 'description', 'category', 'price', 'sku'] },
    { id: 'products_2', name: 'products_inventory.parquet', folder: 'test_data/products', rows: 3000, size: 737280, columns: ['product_id', 'warehouse_id', 'quantity', 'location', 'last_updated'] },
    { id: 'products_3', name: 'products_prices.parquet', folder: 'test_data/products', rows: 2000, size: 491520, columns: ['product_id', 'base_price', 'discount_price', 'currency', 'price_date'] },
    { id: 'products_4', name: 'products_categories.parquet', folder: 'test_data/products', rows: 500, size: 122880, columns: ['category_id', 'category_name', 'parent_category', 'description'] },
    { id: 'products_5', name: 'products_reviews.parquet', folder: 'test_data/products', rows: 10000, size: 2457600, columns: ['review_id', 'product_id', 'user_id', 'rating', 'review_text', 'review_date'] },
  ],
  transactions: [
    { id: 'trans_1', name: 'transactions_jan.parquet', folder: 'test_data/transactions', rows: 25000, size: 6144000, columns: ['transaction_id', 'account_id', 'amount', 'transaction_type', 'date', 'merchant', 'status'] },
    { id: 'trans_2', name: 'transactions_feb.parquet', folder: 'test_data/transactions', rows: 23000, size: 5652480, columns: ['transaction_id', 'account_id', 'amount', 'transaction_type', 'date', 'merchant', 'status', 'fee'] },
    { id: 'trans_3', name: 'transactions_mar.parquet', folder: 'test_data/transactions', rows: 27000, size: 6635520, columns: ['transaction_id', 'account_id', 'amount', 'transaction_type', 'date', 'merchant', 'status', 'fee', 'currency'] },
    { id: 'trans_4', name: 'transactions_apr.parquet', folder: 'test_data/transactions', rows: 26000, size: 6389760, columns: ['transaction_id', 'account_id', 'amount', 'transaction_type', 'date', 'merchant', 'status', 'fee', 'currency', 'location'] },
    { id: 'trans_5', name: 'transactions_may.parquet', folder: 'test_data/transactions', rows: 28000, size: 6881280, columns: ['transaction_id', 'account_id', 'amount', 'transaction_type', 'date', 'merchant', 'status', 'fee', 'currency', 'location', 'metadata'] },
  ],
};

export const getTestFiles = (dataType, count) => {
  const files = TEST_PARQUET_FILES[dataType] || TEST_PARQUET_FILES.sample;
  return files.slice(0, Math.min(count, files.length));
};

