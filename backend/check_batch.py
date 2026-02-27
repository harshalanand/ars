import pyodbc

batch_id = 'UST_30d514aa2c'

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=HOPC560;DATABASE=Claude;UID=sa;PWD=vrl@55555;TrustServerCertificate=yes;'
)
cursor = conn.cursor()

# Check table schema
print('data_change_log columns:')
cursor.execute('''
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'data_change_log'
''')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

print('\naudit_log columns:')
cursor.execute('''
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'audit_log'
''')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

# Check audit_log
print(f'\nLooking for batch_id: {batch_id}')
cursor.execute('''
    SELECT id, table_name, action_type, row_count, changed_by, changed_at, notes, changed_columns, batch_id
    FROM audit_log WHERE batch_id=?
''', (batch_id,))
cols = [d[0] for d in cursor.description]
rows = cursor.fetchall()

if rows:
    print('Audit log entry found:')
    for row in rows:
        for col, val in zip(cols, row):
            print(f'  {col}: {val}')
else:
    print('No audit_log entry found for this batch_id')
    
# Check if data_change_log has any data
cursor.execute('SELECT COUNT(*) FROM data_change_log')
print(f'\nTotal rows in data_change_log: {cursor.fetchone()[0]}')

conn.close()
