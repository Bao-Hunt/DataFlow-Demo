from flask import Flask, render_template, request, jsonify

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


def build_sample_code(source, ingest, sink):
    lines = []
    lines.append("# Mẫu mã Spark Python (tự động sinh)")
    lines.append("# Source: {}".format(source or "<none>"))
    lines.append("# Ingestion: {}".format(ingest or "<none>"))
    lines.append("# Sink: {}".format(sink or "<none>"))
    # Note: source-specific configuration is not included in demo output
    lines.append("")

    imports = set()
    imports.add("from pyspark.sql import SparkSession")

    # Decide imports and snippets based on source
    source_snippet = []
    if source == 'HDFS':
        source_snippet.append("# HDFS read example (CSV)")
        source_snippet.append("df = spark.read.csv(\"hdfs://namenode:9000/path-to-file.csv\", header=True, inferSchema=True)")
        source_snippet.append("# or: df = spark.read.parquet('hdfs://namenode:9000/path-to-file.parquet')")
    elif source == 'RDBMS':
        source_snippet.append("# JDBC read example (RDBMS)")
        source_snippet.append("jdbc_url = \"jdbc:yourdb://host:port/service\"  # ví dụ: jdbc:oracle:thin:@//hostname:1521/service_name")
        source_snippet.append("connection_properties = {\"user\": \"your-user\", \"password\": \"your-password\", \"driver\": \"your.jdbc.Driver\"}")
        source_snippet.append("df = spark.read.jdbc(url=jdbc_url, table=\"TABLE_NAME\", properties=connection_properties)")
    elif source == 'Web API':
        imports.add("import requests")
        source_snippet.append("# Web API batch example (fetch JSON then create DataFrame)")
        source_snippet.append("resp = requests.get(\"https://api.example.com/data\")")
        source_snippet.append("data = resp.json()  # adjust parsing as needed")
        source_snippet.append("df = spark.createDataFrame(data)")
    elif source == 'Web Scraping':
        imports.add("import requests")
        imports.add("from bs4 import BeautifulSoup")
        source_snippet.append("# Web scraping example (simple)")
        source_snippet.append("html = requests.get(\"https://example.com\").text")
        source_snippet.append("soup = BeautifulSoup(html, 'html.parser')")
        source_snippet.append("# parse and build list/dict, then create DataFrame")
        source_snippet.append("# df = spark.createDataFrame(parsed_records)")
    elif source == 'Streaming Kafka':
        source_snippet.append("# Kafka streaming example (Structured Streaming)")
        source_snippet.append("df = spark.readStream.format('kafka')")
        source_snippet.append("df = df.option('kafka.bootstrap.servers', 'broker1:9092')")
        source_snippet.append("df = df.option('subscribe', 'your-topic')")
        source_snippet.append("df = df.load()")
        source_snippet.append("# then parse: df.selectExpr(\"CAST(value AS STRING)\") etc.")
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
        sink_snippet.append("out = df.selectExpr(\"to_json(struct(*)) AS value\")")
        sink_snippet.append("query = out.writeStream.format('kafka')")
        sink_snippet.append("query = query.option('kafka.bootstrap.servers', 'broker1:9092')")
        sink_snippet.append("query = query.option('topic', 'output-topic')")
        sink_snippet.append("query = query.option('checkpointLocation', '/tmp/checkpoint')")
        sink_snippet.append("query = query.start()")
    if sink and ('HDFS' in (sink or '')):
        sink_snippet.append("# Write to HDFS (CSV)")
        sink_snippet.append("df.write.mode('overwrite').csv(\"hdfs://namenode:9000/output/path\")")
    if sink and ('Database' in (sink or '') or 'RDBMS' in (sink or '')):
        sink_snippet.append("# JDBC write example")
        sink_snippet.append("# df.write.jdbc(url=jdbc_url, table=\"TABLE_NAME\", mode=\"append\", properties=connection_properties)")

    if not sink_snippet:
        sink_snippet.append("# Sink placeholder: implement write logic for chosen sink")

    # Build final code
    for imp in sorted(imports):
        lines.append(imp)
    lines.append("")
    lines.append("def main():")
    lines.append("    spark = SparkSession.builder.appName('DataFlowDemo').getOrCreate()")
    lines.append("")
    # add source snippet indented
    for s in source_snippet:
        lines.append("    " + s)
    lines.append("")
    lines.append("    # Processing placeholder")
    lines.append("    # ... transform df as needed ...")
    lines.append("")
    for s in sink_snippet:
        lines.append("    " + s)
    lines.append("")
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
