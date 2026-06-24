from flask import Flask, render_template, request, jsonify

app = Flask(__name__)


@app.route('/')
def index():
    DEFAULT_FAQS = [
        {
            "question": "Các điểm nghẽn thường gặp trong Pipeline khi Streaming",
            "answer": (
                "- Kafka topic không được phân partition hợp lý\n"
                "Giải pháp:\n"
                "  . Tăng số partition khi tạo topic để tăng khả năng song song:\n"
                "    kafka-topics.sh --create \\\n+  --topic my_topic \\\n+  --partitions 6 \\\n+  --replication-factor 3 \\\n+  --bootstrap-server localhost:9092\n\n"
                "  . Dùng key khi producer gửi message:\n"
                "    producer.send(\"topic_name\", key=\"column_name\", value=\"...\")\n"
            )
        },
        {
            "question": "Các vấn đề và giải pháp tối ưu Spark – Database",
            "answer": (
                "a) Phân chia partition khi đọc dữ liệu\n"
                "Vấn đề: Nếu không phân chia partition, Spark chỉ tạo một task duy nhất để đọc toàn bộ dữ liệu → hiệu năng thấp.\n"
                "Giải pháp: Sử dụng cơ chế partitioning của JDBC với các tham số: partitionColumn, lowerBound, upperBound, numPartitions.\n\n"
                "b) Pushdown query về Database\n"
                "Vấn đề: Nếu Spark đọc toàn bộ dữ liệu rồi mới lọc/join, sẽ tốn tài nguyên.\n"
                "Giải pháp: Đẩy các lệnh WHERE/JOIN/AGGREGATE xuống Database để DB xử lý trước.\n\n"
                "c) Fetch Size\n"
                "Giải pháp: Điều chỉnh tham số .option(\"fetchsize\", 10000) để cân bằng round-trip và bộ nhớ.\n\n"
                "d) Lưu trữ trung gian (Staging) & CDC\n"
                "Vấn đề: Đọc trực tiếp từ DB trong streaming có thể gây tải nặng cho hệ giao dịch.\n"
                "Giải pháp: Dùng Staging layer (HDFS/Data Lake) hoặc CDC (Debezium + Kafka) để lấy dữ liệu thay đổi theo thời gian thực."
            )
        },
        {
            "question": "12 bước làm sạch dữ liệu (Oracle SQL)",
            "answer": (
                "Bước 1: Thay thế NULL bằng giá trị mặc định\n"
                "SQL:\n"
                "SELECT NVL(col1, 'Default Value') AS col1_clean\n"
                "FROM Table;\n\n"

                "Bước 2: Xóa record chứa NULL\n"
                "SQL:\n"
                "DELETE FROM Table\n"
                "WHERE col IS NULL;\n\n"

                "Bước 3: Tìm dòng trùng lặp\n"
                "SQL:\n"
                "SELECT col_name, COUNT(*)\n"
                "FROM Table\n"
                "GROUP BY col_name\n"
                "HAVING COUNT(*) > 1;\n\n"

                "Bước 4: Xóa dòng trùng lặp, giữ lại bản ghi nhỏ nhất theo id\n"
                "SQL:\n"
                "DELETE FROM Table\n"
                "WHERE id NOT IN (SELECT MIN(id) FROM Table GROUP BY col);\n\n"

                "Bước 5: Sửa giá trị sai / chuẩn hóa dữ liệu\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = 'CorrectValue'\n"
                "WHERE col_name = 'WrongValue';\n\n"

                "Bước 6: Chuyển text sang lowercase\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = LOWER(col_name);\n\n"

                "Bước 7: Phát hiện outlier\n"
                "SQL:\n"
                "SELECT *\n"
                "FROM Table\n"
                "WHERE col_name > upper_limit OR col_name < lower_limit;\n\n"

                "Bước 8: Loại bỏ outlier\n"
                "SQL:\n"
                "DELETE FROM Table\n"
                "WHERE col_name > upper_limit OR col_name < lower_limit;\n\n"

                "Bước 9: Xóa khoảng trắng thừa\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = TRIM(col_name);\n\n"

                "Bước 10: Tách fullname thành first_name và last_name\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET first_name = SUBSTR(full_name, 1, INSTR(full_name, ' ') - 1),\n"
                "    last_name = SUBSTR(full_name, INSTR(full_name, ' ') + 1);\n\n"

                "Bước 11: Chuẩn hóa kiểu dữ liệu ngày tháng\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET date_col = TO_DATE(date_col, 'YYYY-MM-DD');\n\n"

                "Bước 12: Loại bỏ ký tự đặc biệt\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = REGEXP_REPLACE(col_name, '[^a-zA-Z0-9]', '');\n"
            )
        }
        ,
        {
            "question": "Làm sao join hai bảng hàng tỷ dòng trong Spark",
            "answer": (
                "Các chiến lược tối ưu join dữ liệu lớn trong Spark:\n\n"
                "1. Bucket By theo join key\n"
                "Khi ghi dữ liệu xuống HDFS, cấu hình bucketBy cho cả hai bảng theo cùng một join key và cùng số lượng bucket.\n"
                "Nhờ đó, các bản ghi có cùng key sẽ được phân bổ vào cùng bucket trên đĩa.\n"
                "Khi Spark thực hiện join, nó chỉ cần đọc các cặp bucket tương ứng vào RAM để join, không cần shuffle toàn bộ dữ liệu → giảm mạnh chi phí mạng và thời gian.\n\n"
                "2. Partition By theo cột join\n"
                "Nếu cả hai bảng đều được partitionBy theo cùng một cột (ví dụ join key hoặc cột thời gian), Spark có thể tận dụng việc đọc dữ liệu theo partition để giảm khối lượng dữ liệu cần xử lý.\n"
                "Điều này đặc biệt hữu ích khi join theo cột có tính phân vùng tự nhiên (ví dụ ngày, tháng).\n\n"
                "3. Kết hợp partitionBy và bucketBy (ví dụ):\n"
                "bigDF.write\n"
                "    .partitionBy(\"date\")\n"
                "    .bucketBy(200, \"join_key\")\n"
                "    .sortBy(\"join_key\")\n"
                "    .saveAsTable(\"big_table_partitioned_bucketed\")\n\n"
                "mediumDF.write\n"
                "    .partitionBy(\"date\")\n"
                "    .bucketBy(200, \"join_key\")\n"
                "    .sortBy(\"join_key\")\n"
                "    .saveAsTable(\"medium_table_partitioned_bucketed\")\n\n"
                "joinedDF = spark.table(\"big_table_partitioned_bucketed\")\n"
                "    .join(spark.table(\"medium_table_partitioned_bucketed\"), [\"join_key\"])\n\n"
                "4. Lọc sớm nhất có thể (Predicate Pushdown)\n"
                "Áp dụng filter, where, select ngay từ đầu pipeline để loại bỏ dữ liệu dư thừa trước khi join.\n\n"
                "Ví dụ:\n"
                "filteredBig = bigDF\n"
                "    .filter(bigDF.date >= \"2026-01-01\")\n"
                "    .select(\"join_key\", \"colA\", \"colB\")\n\n"
                "filteredMedium = mediumDF\n"
                "    .filter(mediumDF.status == \"active\")\n"
                "    .select(\"join_key\", \"colX\")\n\n"
                "joinedDF = filteredBig.join(filteredMedium, [\"join_key\"])"
            )
        }
    ]
    return render_template('index.html', default_faqs=DEFAULT_FAQS)


def build_sample_code(source, ingest, sink):
    lines = []
    lines.append("# Source: {}".format(source or "<none>"))
    lines.append("# Ingestion: {}".format(ingest or "<none>"))
    lines.append("# Sink: {}".format(sink or "<none>"))
    # Note: source-specific configuration is not included in demo output
    lines.append("")

    # Place certain helpful notes directly under the header (below '# Sink: ...')
    if source == 'Web Scraping':
        lines.append("# Note: Web scraping commonly uses Python (BeautifulSoup / Scrapy) and writes results to Flat files / HDFS / DB")
    if source == 'RDBMS' and ingest == 'Streaming':
        lines.append("# Recommendation: For streaming from a Database, use CDC (Debezium) to publish changes to Kafka and set Source = 'CDC (Debezium)'.")
    if source == 'Web API' and ingest == 'Streaming' and sink and any(x in (sink or '').lower() for x in ['flat', 'database', 'hdfs', 'kafka']):
        lines.append("# Note: this pipeline is often implemented as a batch job scheduled via Airflow")

    # Feasibility label (Vietnamese) shown before the main code
    def feasibility_label_vn(src, ing, snk):
        # Web Scraping is only low-feasibility for streaming, batch is fine
        if src == 'Web Scraping' and ing == 'Streaming':
            return 'Ít khả thi (web scraping dễ bị lỗi cho streaming)'
        # For RDBMS + Streaming, recommend CDC (Debezium -> Kafka)
        if src == 'RDBMS' and ing == 'Streaming':
            return 'Khả thi (khuyến nghị CDC - Debezium → Kafka để stream thay đổi từ DB)'
        if src == 'Web API' and ing == 'Streaming':
            return 'Ít khả thi (API web thường phù hợp chạy theo batch)'
        return 'Khả thi'

    lines.append("# Khả thi: {}".format(feasibility_label_vn(source, ingest, sink)))
    lines.append("")

    imports = set()
    imports.add("from pyspark.sql import SparkSession")
    # add Trigger import when using streaming
    if ingest == 'Streaming':
        imports.add("from pyspark.sql.streaming import Trigger")

    # Decide imports and snippets based on source
    source_snippet = []
    shared_jdbc = False
    # normalize dataframe variable names based on ingestion mode
    main_df = 'df_stream' if ingest == 'Streaming' else 'df'
    parsed_df = main_df
    if source == 'HDFS':
        if ingest == 'Streaming':
            source_snippet.append("# HDFS read streaming example (CSV)")
            source_snippet.append("# define `schema` for the CSV as needed before using readStream")
            source_snippet.append(f"{main_df} = spark.readStream.format(\"csv\") \\")
            source_snippet.append("    .option(\"header\", True) \\")
            source_snippet.append("    .option(\"maxFilesPerTrigger\", 5) \\")
            source_snippet.append("    .option(\"latestFirst\", True) \\")
            source_snippet.append("    .schema(schema) \\")
            source_snippet.append(f"    .load(\"hdfs://namenode:9000/path/input\")")
        else:
            source_snippet.append("# HDFS read example (CSV)")
            source_snippet.append(f"{main_df} = spark.read.csv(\"hdfs://namenode:9000/path-to-file.csv\", header=True, inferSchema=True)")
            source_snippet.append(f"# or: {main_df} = spark.read.parquet('hdfs://namenode:9000/path-to-file.parquet')")
    elif source == 'RDBMS':
        # When source is RDBMS, expose JDBC config so users see jdbc_url and properties
        shared_jdbc = True
        source_snippet.append("# JDBC read example (RDBMS)")
        source_snippet.append("jdbc_url = \"jdbc:postgresql://dbserver:5432/mydatabase\"  # adjust for your DB")
        source_snippet.append("connection_properties = {\"user\": \"dbuser\", \"password\": \"dbpassword\", \"driver\": \"org.postgresql.Driver\"}")
        source_snippet.append(f"{main_df} = spark.read.jdbc(url=jdbc_url, table=\"TABLE_NAME\", properties=connection_properties)")
    elif source == 'Web API':
        imports.add("import requests")
        source_snippet.append("# Web API batch example (fetch JSON then create DataFrame)")
        source_snippet.append("resp = requests.get(\"https://api.example.com/data\")")
        source_snippet.append("data = resp.json()  # adjust parsing as needed")
        source_snippet.append(f"{main_df} = spark.createDataFrame(data)")
    elif source == 'Web Scraping':
        imports.add("import requests")
        imports.add("from bs4 import BeautifulSoup")
        source_snippet.append("# Web scraping example (simple)")
        source_snippet.append("html = requests.get(\"https://example.com\").text")
        source_snippet.append("soup = BeautifulSoup(html, 'html.parser')")
        source_snippet.append("# parse and build list/dict, then create DataFrame")
        source_snippet.append(f"# {main_df} = spark.createDataFrame(parsed_records)")
    elif source == 'Streaming Kafka':
        source_snippet.append("# Kafka streaming example (Structured Streaming)")
        source_snippet.append(f"{main_df} = spark.readStream.format('kafka')")
        source_snippet.append(f"{main_df} = {main_df}.option('kafka.bootstrap.servers', 'broker1:9092')")
        source_snippet.append(f"{main_df} = {main_df}.option('subscribe', 'your-topic')")
        source_snippet.append(f"{main_df} = {main_df}.load()")
        source_snippet.append(f"# then parse: {main_df}.selectExpr(\"CAST(value AS STRING)\") etc.")
    elif source == 'CDC (Debezium)':
        source_snippet.append("# CDC (Debezium) example: Debezium writes DB changes to Kafka topics")
        source_snippet.append("# read Debezium topic via Spark Structured Streaming (see Kafka example)")
    elif source == 'Logs/Monitoring' or source == 'IoT/Sensor':
        source_snippet.append("# Logs/IoT example: simple socket streaming or Kafka")
        source_snippet.append("# df = spark.readStream.format('socket').option('host','localhost').option('port',9999).load()")
    else:
        source_snippet.append("# Source: no specific snippet available; customize as needed")

    # Sink snippets
    sink_snippet = []
    # Prefer Kafka sink snippet when sink indicates Kafka
    if sink and 'kafka' in (sink or '').lower():
        sink_snippet.append("# Kafka streaming sink example")
        sink_snippet.append(f"out = {main_df}.selectExpr(\"to_json(struct(*)) AS value\")")
        # concise chained writeStream call (single-line compact form)
        sink_snippet.append("query = out.writeStream.format('kafka').option('kafka.bootstrap.servers', 'broker1:9092').option('topic', 'output-topic').option('checkpointLocation', '/tmp/checkpoint').start()")
    # Streaming -> HDFS: use streaming write when ingestion is Streaming
    if ingest == 'Streaming' and sink and ('HDFS' in (sink or '')):
        sink_snippet.append("# Write streaming to HDFS (parquet)")
        sink_snippet.append("from pyspark.sql.streaming import Trigger")
        sink_snippet.append(f"query = {main_df}.writeStream \\")
        sink_snippet.append("    .format(\"parquet\") \\")
        sink_snippet.append("    .option(\"path\", \"hdfs://namenode:9000/path/output\") \\")
        sink_snippet.append("    .option(\"checkpointLocation\", \"hdfs://namenode:9000/path/checkpoint\") \\")
        sink_snippet.append("    .outputMode(\"append\") \\")
        sink_snippet.append("    .trigger(Trigger.ProcessingTime(\"10 seconds\")) \\")
        sink_snippet.append("    .start()")
        sink_snippet.append("")
        sink_snippet.append("query.awaitTermination()")
    elif sink and ('HDFS' in (sink or '')):
        sink_snippet.append("# Write to HDFS (CSV)")
        sink_snippet.append("df.write.mode('overwrite').csv(\"hdfs://namenode:9000/output/path\")")
    if sink and ('flat' in (sink or '').lower() or 'flat file' in (sink or '').lower()):
        sink_snippet.append("# Write to flat file (CSV)")
        sink_snippet.append("df.write.mode('overwrite').csv(\"/path/to/output.csv\")  # or .parquet(...)")
    if ingest == 'Streaming' and sink and ('Database' in (sink or '') or 'RDBMS' in (sink or '')):
        # Streaming write to DB via foreachBatch
        sink_snippet.append("# Hàm ghi batch vào Database")
        sink_snippet.append("def write_to_db(batch_df, batch_id):")
        sink_snippet.append("    batch_df.write.jdbc(")
        sink_snippet.append("        url=\"jdbc:postgresql://dbserver:5432/mydatabase\",")
        sink_snippet.append("        table=\"target_table\",")
        sink_snippet.append("        mode=\"append\",")
        sink_snippet.append("        properties={")
        sink_snippet.append("            \"user\": \"dbuser\",")
        sink_snippet.append("            \"password\": \"dbpassword\",")
        sink_snippet.append("            \"driver\": \"org.postgresql.Driver\"")
        sink_snippet.append("        }")
        sink_snippet.append("    )")
        sink_snippet.append("")
        sink_snippet.append("# Cấu hình writeStream với foreachBatch")
        sink_snippet.append(f"query = {parsed_df}.writeStream \\")
        sink_snippet.append("    .foreachBatch(write_to_db) \\")
        sink_snippet.append("    .outputMode(\"append\") \\")
        sink_snippet.append("    .trigger(Trigger.ProcessingTime(\"10 seconds\")) \\")
        sink_snippet.append("    .option(\"checkpointLocation\", \"/tmp/checkpoint_db\") \\")
        sink_snippet.append("    .start()")
        sink_snippet.append("")
        sink_snippet.append("query.awaitTermination()")
    elif sink and ('Database' in (sink or '') or 'RDBMS' in (sink or '')):
        sink_snippet.append("# JDBC write example (example config)")
        # If the source already exposed JDBC config (RDBMS), reuse those variables
        if shared_jdbc:
            sink_snippet.append("df.write.jdbc(url=jdbc_url, table=\"TABLE_NAME\", mode=\"append\", properties=connection_properties)")
        else:
            sink_snippet.append("jdbc_url = \"jdbc:postgresql://dbserver:5432/mydatabase\"")
            sink_snippet.append("connection_properties = {\"user\": \"dbuser\", \"password\": \"dbpassword\", \"driver\": \"org.postgresql.Driver\"}")
            sink_snippet.append("df.write.jdbc(url=jdbc_url, table=\"TABLE_NAME\", mode=\"append\", properties=connection_properties)")

    if not sink_snippet:
        sink_snippet.append("# Sink placeholder: implement write logic for chosen sink")

    # Build final code
    for imp in sorted(imports):
        lines.append(imp)
    lines.append("")
    lines.append("def main():")
    # Build SparkSession with optional configs depending on combo
    builder_parts = ["SparkSession.builder.appName('DataFlowDemo')"]
    need_postgres_jar = (source == 'HDFS' and ingest == 'Batch' and sink and ('Database' in sink or 'RDBMS' in sink))
    need_kafka_packages = (source == 'HDFS' and ingest == 'Batch' and sink and ('HDFS' in sink)) or (sink and 'kafka' in (sink or '').lower())
    if need_postgres_jar:
        builder_parts.append(".config(\"spark.jars\", \"/path/to/postgresql-connector.jar\")")
    if need_kafka_packages:
        builder_parts.append(".config(\"spark.jars.packages\", \"org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0\")")
    builder_line = "    spark = " + "".join(builder_parts) + ".getOrCreate()"
    lines.append(builder_line)
    lines.append("")
    # add source snippet indented
    for s in source_snippet:
        lines.append("    " + s)
    lines.append("")
    lines.append("    # Processing placeholder")
    lines.append("    # ... transform df as needed ...")
    lines.append("")
    # (Top-of-file notes already include Airflow / Web Scraping guidance when relevant)

    for s in sink_snippet:
        lines.append("    " + s)
    lines.append("")

    # (Feasibility note moved to header in Vietnamese)
    lines.append("    spark.stop()")
    lines.append("")
    lines.append("if __name__ == '__main__':")
    lines.append("    main()")
    return "\n".join(lines)


@app.route('/api/generate', methods=['POST'])
def generate():
    data = request.json or {}
    source = data.get('source')
    ingest = data.get('ingest')
    sink = data.get('sink')
    sample = build_sample_code(source, ingest, sink)
    return jsonify({"code": sample})


@app.route('/faq/<int:fid>')
def faq_page(fid):
    DEFAULT_FAQS = [
        {
            "question": "Các điểm nghẽn thường gặp trong Pipeline khi Streaming",
            "answer": (
                "- Kafka topic không được phân partition hợp lý\n"
                "Giải pháp:\n"
                "  . Tăng số partition khi tạo topic để tăng khả năng song song:\n"
                "    kafka-topics.sh --create \\\n+  --topic my_topic \\\n+  --partitions 6 \\\n+  --replication-factor 3 \\\n+  --bootstrap-server localhost:9092\n\n"
                "  . Dùng key khi producer gửi message:\n"
                "    producer.send(\"topic_name\", key=\"column_name\", value=\"...\")\n"
            )
        },
        {
            "question": "Các vấn đề và giải pháp tối ưu Spark – Database",
            "answer": (
                "a) Phân chia partition khi đọc dữ liệu\n"
                "Vấn đề: Nếu không phân chia partition, Spark chỉ tạo một task duy nhất để đọc toàn bộ dữ liệu → hiệu năng thấp.\n"
                "Giải pháp: Sử dụng cơ chế partitioning của JDBC với các tham số: partitionColumn, lowerBound, upperBound, numPartitions.\n\n"
                "b) Pushdown query về Database\n"
                "Vấn đề: Nếu Spark đọc toàn bộ dữ liệu rồi mới lọc/join, sẽ tốn tài nguyên.\n"
                "Giải pháp: Đẩy các lệnh WHERE/JOIN/AGGREGATE xuống Database để DB xử lý trước.\n\n"
                "c) Fetch Size\n"
                "Giải pháp: Điều chỉnh tham số .option(\"fetchsize\", 10000) để cân bằng round-trip và bộ nhớ.\n\n"
                "d) Lưu trữ trung gian (Staging) & CDC\n"
                "Vấn đề: Đọc trực tiếp từ DB trong streaming có thể gây tải nặng cho hệ giao dịch.\n"
                "Giải pháp: Dùng Staging layer (HDFS/Data Lake) hoặc CDC (Debezium + Kafka) để lấy dữ liệu thay đổi theo thời gian thực."
            )
        },
        {
            "question": "12 bước làm sạch dữ liệu (Oracle SQL)",
            "answer": (
                "Bước 1: Thay thế NULL bằng giá trị mặc định\n"
                "SQL:\n"
                "SELECT NVL(col1, 'Default Value') AS col1_clean\n"
                "FROM Table;\n\n"

                "Bước 2: Xóa record chứa NULL\n"
                "SQL:\n"
                "DELETE FROM Table\n"
                "WHERE col IS NULL;\n\n"

                "Bước 3: Tìm dòng trùng lặp\n"
                "SQL:\n"
                "SELECT col_name, COUNT(*)\n"
                "FROM Table\n"
                "GROUP BY col_name\n"
                "HAVING COUNT(*) > 1;\n\n"

                "Bước 4: Xóa dòng trùng lặp, giữ lại bản ghi nhỏ nhất theo id\n"
                "SQL:\n"
                "DELETE FROM Table\n"
                "WHERE id NOT IN (SELECT MIN(id) FROM Table GROUP BY col);\n\n"

                "Bước 5: Sửa giá trị sai / chuẩn hóa dữ liệu\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = 'CorrectValue'\n"
                "WHERE col_name = 'WrongValue';\n\n"

                "Bước 6: Chuyển text sang lowercase\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = LOWER(col_name);\n\n"

                "Bước 7: Phát hiện outlier\n"
                "SQL:\n"
                "SELECT *\n"
                "FROM Table\n"
                "WHERE col_name > upper_limit OR col_name < lower_limit;\n\n"

                "Bước 8: Loại bỏ outlier\n"
                "SQL:\n"
                "DELETE FROM Table\n"
                "WHERE col_name > upper_limit OR col_name < lower_limit;\n\n"

                "Bước 9: Xóa khoảng trắng thừa\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = TRIM(col_name);\n\n"

                "Bước 10: Tách fullname thành first_name và last_name\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET first_name = SUBSTR(full_name, 1, INSTR(full_name, ' ') - 1),\n"
                "    last_name = SUBSTR(full_name, INSTR(full_name, ' ') + 1);\n\n"

                "Bước 11: Chuẩn hóa kiểu dữ liệu ngày tháng\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET date_col = TO_DATE(date_col, 'YYYY-MM-DD');\n\n"

                "Bước 12: Loại bỏ ký tự đặc biệt\n"
                "SQL:\n"
                "UPDATE Table\n"
                "SET col_name = REGEXP_REPLACE(col_name, '[^a-zA-Z0-9]', '');\n"
            )
        }
        ,
        {
            "question": "Làm sao join hai bảng hàng tỷ dòng trong Spark",
            "answer": (
                "Các chiến lược tối ưu join dữ liệu lớn trong Spark:\n\n"
                "1. Bucket By theo join key\n"
                "Khi ghi dữ liệu xuống HDFS, cấu hình bucketBy cho cả hai bảng theo cùng một join key và cùng số lượng bucket.\n"
                "Nhờ đó, các bản ghi có cùng key sẽ được phân bổ vào cùng bucket trên đĩa.\n"
                "Khi Spark thực hiện join, nó chỉ cần đọc các cặp bucket tương ứng vào RAM để join, không cần shuffle toàn bộ dữ liệu → giảm mạnh chi phí mạng và thời gian.\n\n"
                "2. Partition By theo cột join\n"
                "Nếu cả hai bảng đều được partitionBy theo cùng một cột (ví dụ join key hoặc cột thời gian), Spark có thể tận dụng việc đọc dữ liệu theo partition để giảm khối lượng dữ liệu cần xử lý.\n"
                "Điều này đặc biệt hữu ích khi join theo cột có tính phân vùng tự nhiên (ví dụ ngày, tháng).\n\n"
                "3. Kết hợp partitionBy và bucketBy (ví dụ):\n"
                "bigDF.write\n"
                "    .partitionBy(\"date\")\n"
                "    .bucketBy(200, \"join_key\")\n"
                "    .sortBy(\"join_key\")\n"
                "    .saveAsTable(\"big_table_partitioned_bucketed\")\n\n"
                "mediumDF.write\n"
                "    .partitionBy(\"date\")\n"
                "    .bucketBy(200, \"join_key\")\n"
                "    .sortBy(\"join_key\")\n"
                "    .saveAsTable(\"medium_table_partitioned_bucketed\")\n\n"
                "joinedDF = spark.table(\"big_table_partitioned_bucketed\")\n"
                "    .join(spark.table(\"medium_table_partitioned_bucketed\"), [\"join_key\"])\n\n"
                "4. Lọc sớm nhất có thể (Predicate Pushdown)\n"
                "Áp dụng filter, where, select ngay từ đầu pipeline để loại bỏ dữ liệu dư thừa trước khi join.\n\n"
                "Ví dụ:\n"
                "filteredBig = bigDF\n"
                "    .filter(bigDF.date >= \"2026-01-01\")\n"
                "    .select(\"join_key\", \"colA\", \"colB\")\n\n"
                "filteredMedium = mediumDF\n"
                "    .filter(mediumDF.status == \"active\")\n"
                "    .select(\"join_key\", \"colX\")\n\n"
                "joinedDF = filteredBig.join(filteredMedium, [\"join_key\"])"
            )
        }
    ]
    if fid < 0 or fid >= len(DEFAULT_FAQS):
        return "FAQ không tồn tại", 404
    return render_template('faq.html', faq=DEFAULT_FAQS[fid])


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
