# CHƯƠNG 4. XÂY DỰNG BÀI TOÁN TOP-K POPULAR ITEMS BẰNG MAPREDUCE VÀ SPARK

## 4.1. Giới thiệu bài toán

Trong các hệ thống thương mại điện tử, công cụ tìm kiếm và mạng xã hội, bài toán xác định K phần tử xuất hiện nhiều nhất (Top-K Popular Items) có giá trị thực tiễn cao. Ví dụ điển hình là xác định K từ khóa được tìm kiếm nhiều nhất, K sản phẩm bán chạy nhất hay K hashtag phổ biến nhất trong một khoảng thời gian. Khác với bài toán sắp xếp toàn bộ dữ liệu ở Chương 3 — nơi mọi bản ghi hợp lệ đều phải được sắp xếp — bài toán Top-K chỉ quan tâm tới K phần tử đứng đầu theo tiêu chí tần suất giảm dần.

Nếu áp dụng cách tiếp cận naive là sắp xếp toàn bộ tập dữ liệu theo tần suất rồi lấy K dòng đầu, hệ thống sẽ phải shuffle và sắp xếp toàn bộ từ điển phần tử, gây lãng phí tài nguyên đáng kể khi số lượng phần tử phân biệt D rất lớn so với K (D >> K). Mục tiêu của chương này là xây dựng một giải pháp hiệu quả, chỉ shuffle một lượng dữ liệu tỉ lệ với K thay vì tỉ lệ với D.

Để làm rõ sự khác biệt giữa các mô hình xử lý, cùng một bài toán được cài đặt trên hai engine trên cùng một tập dữ liệu:

**Bảng 4.1. Hai cài đặt của bài toán Top-K**

| Engine | Thư mục | Lớp chính | Công nghệ |
|---|---|---|---|
| Hadoop MapReduce | `top-k/` | `com.example.hadoop.TopKItemsJob` | MapReduce 2 giai đoạn, min-heap kích thước K |
| Spark SQL | `spark-top-k/` | `com.example.spark.SparkTopKDF` | DataFrame API (`groupBy` + `orderBy` + `limit`) |

Cả hai cài đặt đọc cùng một tệp HDFS `/training/top-k/input/keywords.txt` và sinh ra cùng một ngữ nghĩa: K cặp `(item, count)` có `count` lớn nhất, sắp xếp theo `count` giảm dần, các trường hợp đồng hạng (bằng count) được phá thế bằng tên `item` tăng dần để kết quả có tính xác định (deterministic) qua các lần chạy.

Tập dữ liệu mẫu `keywords.txt` gồm 19 dòng, phân bố như sau: `laptop` xuất hiện 6 lần, `phone` 5 lần, `tablet` 4 lần và bốn item `watch`, `monitor`, `keyboard`, `headphones` mỗi item xuất hiện 1 lần. Với K = 3, đáp án đúng của bài toán là:

```
laptop    6
phone     5
tablet    4
```

Chương này trình bày phân tích, thiết kế, cài đặt và thực nghiệm của cả hai cài đặt trên cụm Docker Compose đã dựng ở Chương 1.

## 4.2. Phân tích bài toán

### 4.2.1. Đặc thù dữ liệu

Mỗi dòng của tệp đầu vào biểu diễn một lần xuất hiện của một item. Sau khi loại bỏ khoảng trắng ở đầu và cuối dòng bằng `trim()`, dòng rỗng được bỏ qua và thống kê vào counter `EMPTY_LINES`. Khác với bài toán sắp xếp số nguyên ở Chương 3 — nơi dòng không phải số nguyên được coi là `MALFORMED_LINES` — bài toán Top-K làm việc trên chuỗi ký tự tự do nên mọi dòng không rỗng đều là một item hợp lệ. Tuy nhiên, ở giai đoạn 2 (giai đoạn chọn Top-K), nếu dòng kết quả đếm có định dạng không hợp lệ (không chứa ký tự phân cách `\t` hoặc count không phải số), dòng đó được bỏ qua và counter `MALFORMED_LINES` được tăng.

### 4.2.2. Mô hình hóa theo MapReduce

Bài toán được phân rã thành hai bài toán con nối tiếp nhau:

1.  **Bài toán đếm tần suất (Counting):** tương tự bài toán WordCount kinh điển. Mỗi dòng được biến đổi thành cặp `(item, 1)`, sau đó các giá trị có cùng khóa `item` được cộng dồn để thu được cặp `(item, count)`.

2.  **Bài toán chọn Top-K (Selection):** từ tập hợp các cặp `(item, count)`, chỉ giữ lại K cặp có `count` lớn nhất. Đây là bài toán Top-K cổ điển, có thể giải hiệu quả bằng cấu trúc heap mà không cần sắp xếp toàn cục toàn bộ từ điển.

Nếu sử dụng một Reducer duy nhất để sắp xếp toàn bộ từ điển theo `count`, lượng dữ liệu shuffle sẽ là O(D) với D là số item phân biệt. Giải pháp heap được đề xuất trong chương này chỉ shuffle tối đa M × K bản ghi (M là số Mapper), giảm đáng kể khi từ điển lớn. Đây chính là bài học trọng tâm của bài toán Top-K: tránh sắp xếp toàn cục khi chỉ cần K phần tử đầu.

### 4.2.3. Thuật toán heap và các thứ tự so sánh

Mỗi task (Mapper hoặc Reducer ở giai đoạn 2) duy trì một `PriorityQueue<ItemCount>` hoạt động như min-heap với kích thước tối đa K:

```
với mỗi candidate (item, count):
    nếu heap.size < K:
        đưa candidate vào heap
    ngược lại nếu candidate > heap.min (theo HEAP_ORDER):
        loại bỏ phần tử nhỏ nhất khỏi heap
        đưa candidate vào heap
```

Hai thứ tự so sánh được định nghĩa tách biệt trong `TopKItemsJob.java`:

*   **HEAP_ORDER** (thứ tự trong heap): so sánh `count` tăng dần; nếu `count` bằng nhau thì `item` có tên lớn hơn được coi là "nhỏ hơn" để bị loại trước. Điều này đảm bảo tính xác định: trong các trường hợp đồng hạng, item có tên nhỏ hơn được ưu tiên giữ lại.

*   **OUTPUT_ORDER** (thứ tự đầu ra cuối cùng): so sánh `count` giảm dần; nếu bằng nhau thì `item` tăng dần theo thứ tự từ điển.

Độ phức tạp bộ nhớ của mỗi task là O(K), không phụ thuộc vào kích thước tập đầu vào. Đây là ưu điểm then chốt giúp hệ thống mở rộng tốt khi dữ liệu tăng lớn.

## 4.3. Thiết kế luồng thực thi

### 4.3.1. Giai đoạn 1 — Đếm tần suất (Counting Stage)

Giai đoạn này thực hiện chức năng WordCount có sử dụng Combiner để giảm lưu lượng shuffle:

```
Input split → CountMapper(item, 1) → Combiner (CountReducer) → Shuffle theo item → CountReducer (sum) → (item \t count)
```

*   **CountMapper** (`TopKItemsJob.java:83`): đọc từng dòng, thực hiện `trim()`, bỏ qua dòng rỗng (tăng `EMPTY_LINES`), phát cặp `(Text(item), LongWritable(1))`.
*   **CountReducer** (`TopKItemsJob.java:101`): cộng dồn các giá trị `1` cho mỗi `item`. Lớp này được cấu hình đồng thời làm `Combiner` (`job.setCombinerClass(CountReducer.class)`) để tiền tổng hợp ngay trong tiến trình Mapper, giảm đáng kể dữ liệu trung gian truyền qua mạng.
*   Kết quả trung gian được ghi ra thư mục tạm `output-counting-tmp` dưới dạng các dòng `item \t count`.

### 4.3.2. Giai đoạn 2 — Chọn Top-K (Selection Stage)

```
(item \t count) → TopKMapper (min-heap K, emit ≤ K) → Shuffle (LongWritable count, Text item) → 1 TopKReducer (min-heap K toàn cục) → sắp OUTPUT_ORDER → (item \t count) giảm dần
```

*   **TopKMapper** (`TopKItemsJob.java:149`): trong `setup()` khởi tạo heap kích thước K; trong `map()` gọi `parseCountLine()` để tách dòng bằng `lastIndexOf('\t')`, kiểm tra `count ≥ 0`, đưa vào heap qua `offerBounded()`; trong `cleanup()` chỉ phát ra nội dung heap (tối đa K bản ghi) dưới dạng `(LongWritable(count), Text(item))`. Dòng không hợp lệ được đếm vào `MALFORMED_LINES`.
*   **TopKReducer** (`TopKItemsJob.java:183`): gom toàn bộ ứng viên từ các Mapper (tối đa M × K bản ghi) vào một heap toàn cục kích thước K bằng cùng thuật toán `offerBounded()`; trong `cleanup()` sao chép heap ra danh sách, sắp xếp theo `OUTPUT_ORDER` và ghi ra HDFS dưới dạng `(Text(item), LongWritable(count))`.
*   `job.setNumReduceTasks(1)` được thiết lập bắt buộc ở `createTopKJob()` để đảm bảo kết quả là Top-K toàn cục duy nhất, thay vì Top-K cục bộ của từng partition.

### 4.3.3. Sơ đồ xử lý tổng thể

```
Hình 4.1. Sơ đồ xử lý hai giai đoạn của bài toán Top-K Popular Items

                        ┌─────────────────────┐
                        │  HDFS keywords.txt  │
                        │  (19 dòng mẫu)      │
                        └─────────┬───────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   GIAI ĐOẠN 1: COUNTING   │
                    │  CountMapper (item, 1)    │
                    │  Combiner (tiền tổng hợp) │
                    │  CountReducer (sum)       │
                    └─────────────┬─────────────┘
                                  │  (item \t count)
                    ┌─────────────▼─────────────┐
                    │ GIAI ĐOẠN 2: SELECTION    │
                    │ TopKMapper (heap K) ─┬── ≤K ──►│
                    │ TopKMapper (heap K) ─┼── ≤K ──►│ 1 TopKReducer │
                    │ TopKMapper (heap K) ─┴── ≤K ──►│ (heap K toàn cục)│
                    └─────────────────────────┬────────┘
                                              │ sắp OUTPUT_ORDER
                                    ┌─────────▼─────────┐
                                    │ HDFS Top-K output │
                                    │ count giảm dần    │
                                    └───────────────────┘
```

Tương đương trên Spark, toàn bộ hai giai đoạn được biểu diễn bằng ba phép toán DataFrame trong `SparkTopKDF.java:55`:

```java
cleaned = raw.withColumn("item", trim(col("value"))).filter(item != "")
counted = cleaned.groupBy("item").count()
topK    = counted.orderBy(count.desc(), item.asc()).limit(k)
```

Trong đó `groupBy("item").count()` là phép WordCount phân tán của Spark (shuffle kết hợp tiền tổng hợp cục bộ), và `orderBy(...).limit(k)` được Catalyst Optimizer tối ưu thành thao tác Top-K dùng heap nội bộ, không cần sắp xếp toàn bộ DataFrame.

## 4.4. Cài đặt hệ thống

### 4.4.1. Kiến trúc triển khai

Hệ thống kế thừa nguyên vẹn kiến trúc Docker Compose sáu dịch vụ đã trình bày ở Chương 3 (Hình 3.3), tất cả kết nối qua mạng nội bộ `bigdata-net`. Về lưu trữ, cả hai engine truy cập HDFS qua URI `hdfs://namenode:9000`; NameNode quản lý namespace, DataNode lưu các block vật lý. Về điều phối, job MapReduce được gửi tới ResourceManager (khi chạy ở chế độ YARN) hoặc chạy trực tiếp trong JVM client qua LocalJobRunner (chế độ mặc định hiện tại do `hadoop.env` chưa khai báo `mapreduce.framework.name=yarn` — xem Chương 1, mục 7.5). Job Spark chạy trên cụm Spark Standalone (Spark Master/Worker).

```
Hình 4.2. Kiến trúc triển khai Top-K trên Docker Compose (kế thừa Hình 3.3)

    HDFS Layer          YARN Layer         Spark Layer
    ┌──────────┐       ┌──────────────┐   ┌──────────────┐
    │ NameNode │◄──────┤ResourceManager│   │ Spark Master │
    │ :9870    │       │   :8088      │   │   :8080      │
    └────┬─────┘       └──────┬───────┘   └──────┬───────┘
         │                    │                   │
    ┌────▼─────┐       ┌──────▼───────┐   ┌──────▼───────┐
    │ DataNode │       │ NodeManager  │   │ Spark Worker │
    │ :9864    │       │   :8042      │   │  2 core/2GB  │
    └──────────┘       └──────────────┘   └──────────────┘
              └────── bigdata-net (bridge) ──────┘
```

### 4.4.2. Cấu trúc mã nguồn

**Bảng 4.2. Cấu trúc dự án**

| Dự án | Đường dẫn | Tệp chính |
|---|---|---|
| Hadoop MapReduce | `top-k/` | `src/main/java/com/example/hadoop/TopKItemsJob.java` |
| Spark SQL | `spark-top-k/` | `src/main/java/com/example/spark/SparkTopKDF.java` |

Các hằng và phương thức đáng chú ý trong `TopKItemsJob.java`:

*   `TOP_K_CONF_KEY = "topk.k"` — khóa cấu hình truyền tham số K qua `Configuration` (`TopKItemsJob.java:49`).
*   `HEAP_ORDER` và `OUTPUT_ORDER` — hai Comparator tách biệt cho heap và đầu ra (`TopKItemsJob.java:54,63`).
*   `parseCountLine(String)` — phân tích dòng `item \t count` bằng `lastIndexOf('\t')`, kiểm tra `count ≥ 0` (`TopKItemsJob.java:120`).
*   `offerBounded(PriorityQueue, ItemCount, int)` — thuật toán heap giới hạn K dùng chung cho Mapper và Reducer (`TopKItemsJob.java:140`).
*   `createCountJob()` và `createTopKJob()` — tách biệt việc tạo job hai giai đoạn để có thể kiểm thử độc lập (`TopKItemsJob.java:218,236`).
*   `runTopK()` — điều phối hai giai đoạn, quản lý thư mục tạm `output-counting-tmp` và tự động xóa sau khi hoàn thành (`TopKItemsJob.java:259`).

Trong `SparkTopKDF.java`, phương thức `countAndTakeTopK(Dataset<Row>, int)` (`dòng 55`) đóng gói toàn bộ logic; chế độ ghi `mode("overwrite")` (`dòng 43`) cho phép chạy lại mà không cần xóa thư mục đích thủ công. Tham số dòng lệnh của cả hai engine được thống nhất: `<input> <output> <k>`, giá trị `K ≤ 0` hoặc thiếu tham số đều bị từ chối với mã thoát `2`.

## 4.5. Chuẩn bị dữ liệu HDFS

Cả hai engine đọc chung một tệp duy nhất trên HDFS. Các bước chuẩn bị như sau:

```bash
# Sao chép tệp mẫu từ host vào container NameNode
docker cp top-k/input/keywords.txt namenode:/tmp/keywords.txt

# Tạo thư mục input trên HDFS và đưa tệp lên
docker exec namenode hdfs dfs -mkdir -p /training/top-k/input
docker exec namenode hdfs dfs -put -f \
  /tmp/keywords.txt /training/top-k/input/keywords.txt

# Kiểm tra nội dung trên HDFS
docker exec namenode hdfs dfs -cat /training/top-k/input/keywords.txt
```

Nội dung `keywords.txt` (19 dòng):

```
laptop
phone
tablet
laptop
phone
laptop
watch
laptop
phone
tablet
laptop
phone
tablet
monitor
keyboard
headphones
laptop
phone
tablet
```

Tần suất thống kê: `laptop: 6, phone: 5, tablet: 4, watch: 1, monitor: 1, keyboard: 1, headphones: 1`.

## 4.6. Kết quả thực nghiệm

### 4.6.1. Thực nghiệm Hadoop MapReduce

**Biên dịch:**

```bash
cd top-k
mvn clean package
# Sinh ra target/top-k-1.0-SNAPSHOT.jar
```

**Chạy job (K = 3):**

```bash
docker cp top-k/target/top-k-1.0-SNAPSHOT.jar namenode:/tmp/top-k.jar
docker exec namenode hdfs dfs -rm -r -f /training/top-k/hadoop-output
docker exec namenode hadoop jar /tmp/top-k.jar \
  com.example.hadoop.TopKItemsJob \
  /training/top-k/input \
  /training/top-k/hadoop-output \
  3
```

**Xem kết quả:**

```bash
docker exec namenode hdfs dfs -cat /training/top-k/hadoop-output/part-r-00000
```

**Kết quả thu được (phân cách bằng TAB):**

```
Hình 4.3. Kết quả Hadoop MapReduce với K = 3

laptop    6
phone     5
tablet    4
```

Kết quả được sắp xếp đúng theo `count` giảm dần, các trường hợp đồng hạng được sắp theo tên item tăng dần. Thư mục tạm `hadoop-output-counting-tmp` đã được tự động xóa sau khi job hoàn thành. Counter `EMPTY_LINES` ghi nhận số dòng rỗng bị bỏ qua (nếu có).

Kiểm thử smoke test (không cần Maven trên host) cũng cho kết quả đạt:

```bash
docker cp top-k namenode:/tmp/top-k
docker exec namenode sh -c '
  mkdir -p /tmp/top-k/classes &&
  javac -cp "$(hadoop classpath)" -d /tmp/top-k/classes \
    /tmp/top-k/src/main/java/com/example/hadoop/TopKItemsJob.java \
    /tmp/top-k/src/test/java/com/example/hadoop/TopKItemsJobSmokeTest.java &&
  java -cp "/tmp/top-k/classes:$(hadoop classpath)" \
    com.example.hadoop.TopKItemsJobSmokeTest
'
# PASS: all TopKItemsJob smoke tests
```

### 4.6.2. Thực nghiệm Spark SQL

**Biên dịch:**

```bash
cd spark-top-k
mvn clean package
# Sinh ra target/spark-top-k-1.0-SNAPSHOT.jar
```

**Cấp quyền ghi HDFS cho Spark và nộp job:**

```bash
docker compose up -d namenode datanode spark-master spark-worker
docker exec namenode hdfs dfs -mkdir -p /training/top-k
docker exec namenode hdfs dfs -chown spark:supergroup /training/top-k

docker cp spark-top-k/target/spark-top-k-1.0-SNAPSHOT.jar spark-master:/tmp/spark-top-k.jar
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --class com.example.spark.SparkTopKDF \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /tmp/spark-top-k.jar \
  hdfs://namenode:9000/training/top-k/input/keywords.txt \
  hdfs://namenode:9000/training/top-k/spark-output \
  3
```

**Xem kết quả:**

```bash
docker exec namenode hdfs dfs -cat '/training/top-k/spark-output/part-*.csv'
```

**Kết quả thu được (phân cách bằng dấu phẩy):**

```
Hình 4.4. Kết quả Spark SQL với K = 3

laptop,6
phone,5
tablet,4
```

Spark có thể sinh ra nhiều tệp `part-*.csv` cùng với tệp đánh dấu `_SUCCESS`; chế độ `overwrite` đảm bảo chạy lại chỉ thay thế thư mục đích của Spark.

### 4.6.3. So sánh và đối chiếu hai engine

Để xác minh tính nhất quán, kết quả của hai engine được so sánh trực tiếp sau khi chuẩn hóa định dạng phân cách:

```bash
mkdir -p comparison-output
docker exec namenode hdfs dfs -cat \
  /training/top-k/hadoop-output/part-r-00000 \
  > comparison-output/hadoop.txt
docker exec namenode hdfs dfs -cat \
  '/training/top-k/spark-output/part-*.csv' | tr ',' '\t' \
  > comparison-output/spark.txt
diff -u comparison-output/hadoop.txt comparison-output/spark.txt
# Không có diff — hai engine cho kết quả đồng nhất
```

**Bảng 4.3. So sánh Hadoop MapReduce và Spark SQL trên bài toán Top-K**

| Tiêu chí | Hadoop MapReduce | Spark SQL |
|---|---|---|
| Đầu vào | HDFS text (`keywords.txt`) | Cùng tệp HDFS |
| Đếm tần suất | Mapper `(item,1)` + Combiner + Reducer | `groupBy("item").count()` |
| Chọn Top-K | Min-heap K显式 ở Mapper/Reducer, 1 Reducer tổng hợp | `orderBy(count desc, item asc).limit(k)` |
| Bộ nhớ heap | O(K) mỗi task,显式 | O(K) nội bộ engine |
| Lượng shuffle | Tối đa M × K bản ghi (giai đoạn 2) | Tối ưu bởi Catalyst |
| Dòng rỗng | Bỏ qua, đếm `EMPTY_LINES` | Lọc trong DataFrame |
| Dịch vụ tính toán | LocalJobRunner (mặc định hiện tại) | Spark Standalone Master/Worker |
| Định dạng đầu ra | 1 tệp `part-r-00000` (TAB) | 1..n tệp `part-*.csv` (CSV) + `_SUCCESS` |
| Phù hợp | Pipeline batch bền vững, minh họa rõ cơ chế phân tán | API quan hệ ngắn gọn, bài toán lặp/nhiều giai đoạn |

Nhận xét: cài đặt MapReduce làm bộc lộ rõ thuật toán phân tán (heap cục bộ ở Mapper, heap toàn cục ở Reducer, bộ nhớ giới hạn theo K), trong khi cài đặt Spark đạt cùng đáp án với lượng mã nguồn ít hơn đáng kể nhờ ủy thác shuffle và Top-K cho query engine. Đây là phép so sánh về tính đúng đắn và kiến trúc, không phải một benchmark hiệu năng nghiêm ngặt. Việc cả hai cùng đọc/ghi HDFS qua `hdfs://namenode:9000` trên cùng mạng `bigdata-net` cho thấy khả năng chia sẻ tầng lưu trữ giữa hai hệ sinh thái, như đã phân tích ở Chương 1 (mục 6.2).

## 4.7. Kết chương

Chương 4 đã trình bày việc xây dựng và thực nghiệm bài toán Top-K Popular Items theo hướng tiếp cận hai giai đoạn: đếm tần suất phân tán và chọn Top-K bằng min-heap kích thước cố định K. Thực nghiệm trên cùng tập dữ liệu `keywords.txt` (19 dòng, 7 item phân biệt) cho thấy cả hai cài đặt — Hadoop MapReduce (2 stage, mỗi task heap ≤ K, một Reducer tổng hợp toàn cục) và Spark SQL (`groupBy` + `orderBy` + `limit`) — đều cho ra kết quả Top-3 chính xác là `laptop (6)`, `phone (5)`, `tablet (4)` và khớp nhau tuyệt đối sau khi chuẩn hóa định dạng.

Giải pháp heap đã chứng minh ưu thế so với sắp xếp toàn cục: lượng dữ liệu shuffle ở giai đoạn chọn Top-K được giới hạn ở O(M × K) thay vì O(D), với D là kích thước từ điển. Kết quả này khẳng định tính đúng đắn của phương pháp đề xuất và làm cơ sở cho việc đánh giá tổng kết toàn bộ báo cáo ở chương tiếp theo.

---

## TÀI LIỆU THAM KHẢO (bổ sung cho Chương 4)

[1] Apache Hadoop, *MapReduce Tutorial*, https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
[2] Apache Spark, *Spark SQL, DataFrames and Datasets Guide*, https://spark.apache.org/docs/latest/sql-programming-guide.html
[3] J. Dean và S. Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters," *OSDI'04*, 2004.
