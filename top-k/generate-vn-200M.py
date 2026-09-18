#!/usr/bin/env python3
"""
Gen 200M dòng từ 10k từ tiếng Việt thông dụng, phân bố Zipf.

- Vocab: 10k từ tiếng Việt (lấy từ top-k/input/vocab-10k-vn.txt nếu có,
  không thì tự tạo từ base list mở rộng lên 10k)
- Output: mỗi dòng = 1 từ, 200M dòng ~ 1.5-2GB, stream theo batch

Usage:
  python3 top-k/generate-vn-200M.py                                          # 10k vocab -> 200M dòng
  python3 top-k/generate-vn-200M.py --lines 50000000 --output top-k/input/keywords-50M-vn.txt
  python3 top-k/generate-vn-200M.py --vocab my-vocab.txt --uniform            # vocab riêng / chia đều
  python3 top-k/generate-vn-200M.py --vocab-size 10000 --zipf-s 1.1 --seed 42
"""
import argparse, os, random, sys

parser = argparse.ArgumentParser()
parser.add_argument("--vocab", default="top-k/input/vocab-10k-vn.txt")
parser.add_argument("--vocab-size", type=int, default=10_000)
parser.add_argument("--lines", type=int, default=200_000_000)
parser.add_argument("--output", default="top-k/input/keywords-200M-vn.txt")
parser.add_argument("--uniform", action="store_true", help="chia đều thay vì Zipf")
parser.add_argument("--zipf-s", type=float, default=1.08)
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--batch", type=int, default=1_000_000)
args = parser.parse_args()

random.seed(args.seed)

# ---- 1500 từ tiếng Việt thông dụng (tần suất cao) ----
BASE = """
người nhà yêu thương cuộc sống thời gian công việc học tập gia đình bạn bè tình yêu
hạnh phúc sức khỏe niềm vui hy vọng ước mơ tương lai quá khứ hiện tại đất nước
Việt Nam Hà Nội thành phố nông thôn biển rừng núi sông hồ cây cối hoa lá
mặt trời mặt trăng ngôi sao bầu trời mây mưa gió bão nắng nóng lạnh ấm
ăn uống ngủ nghỉ đi lại nói chuyện lắng nghe thấu hiểu chia sẻ giúp đỡ
cảm ơn xin lỗi tạm biệt hẹn gặp lại chào buổi sáng buổi tối ngày đêm
một hai ba bốn năm sáu bảy tám chín mười trăm nghìn triệu tỷ
tôi bạn anh chị em ông bà cha mẹ con cháu thầy cô học sinh sinh viên
trường lớp sách vở bút thước bảng phấn máy tính điện thoại internet
công nghệ khoa học nghệ thuật âm nhạc hội họa điện ảnh văn học thơ ca
kinh tế chính trị xã hội văn hóa giáo dục y tế giao thông du lịch
thể thao bóng đá cầu lông bơi lội chạy bộ đạp xe du lịch khám phá
món ăn phở bún miến cơm cháo bánh mì bánh chưng bánh tét chả giò
trà cà phê nước mía nước dừa bia rượu trái cây táo cam chuối xoài
áo quần giày dép mũ nón túi xách đồng hồ kính mắt trang sức vàng bạc
nhà cửa phòng khách phòng ngủ bếp ăn vườn sân cổng hàng rào
cửa sổ bàn ghế tủ giường đèn quạt điều hòa tivi tủ lạnh máy giặt
xe máy ô tô xe đạp xe buýt tàu hỏa máy bay thuyền ghe cầu đường
chợ siêu thị cửa hàng bệnh viện trường học công viên bảo tàng thư viện
công ty văn phòng nhà máy xí nghiệp nông trại đồng ruộng vườn tược
mùa xuân mùa hạ mùa thu mùa đông tết trung thu giỗ tổ lễ hội
đám cưới đám hỏi sinh nhật kỷ niệm ngày lễ quốc khánh giải phóng
lịch sử truyền thống văn minh hiện đại phát triển bền vững hội nhập
hòa bình hữu nghị hợp tác cạnh tranh thị trường đầu tư khởi nghiệp
doanh nghiệp khách hàng sản phẩm dịch vụ chất lượng giá cả khuyến mãi
ngân hàng tài chính tiền tệ tín dụng bảo hiểm chứng khoán bất động sản
pháp luật công lý an ninh quốc phòng ngoại giao chính sách chiến lược
môi trường khí hậu thiên nhiên động vật thực vật sinh thái đa dạng
năng lượng điện nước dầu khí than đá mặt trời gió tái tạo tiết kiệm
thông tin truyền thông báo chí xuất bản phát thanh truyền hình mạng xã hội
ngôn ngữ tiếng Việt tiếng Anh tiếng Trung tiếng Nhật tiếng Hàn tiếng Pháp
toán lý hóa sinh sử địa văn anh tin học ngoại ngữ kỹ năng mềm
tư duy sáng tạo đổi mới nghiên cứu thực nghiệm ứng dụng chuyển đổi số
trí tuệ nhân tạo dữ liệu lớn điện toán đám mây blockchain an toàn thông tin
sức mạnh đoàn kết trách nhiệm kỷ luật trung thực dũng cảm kiên trì
nhẫn nại khiêm tốn lễ phép hiếu thảo nhân ái bao dung công bằng
tự tin tự lập tự trọng tự giác chăm chỉ cần cù siêng năng nỗ lực
cố gắng phấn đấu vươn lên vượt khó thành công thất bại bài học
kinh nghiệm trải nghiệm thử thách cơ hội lựa chọn quyết định hành động
kết quả mục tiêu kế hoạch chiến lược tầm nhìn sứ mệnh giá trị cốt lõi
đam mê nhiệt huyết cống hiến hy sinh trách nhiệm nghĩa vụ quyền lợi
tự do dân chủ công bằng văn minh hạnh phúc ấm no phồn vinh
yêu nước yêu đồng bào yêu lao động yêu khoa học yêu thiên nhiên
gia đình dòng họ quê hương làng xóm phố phường cộng đồng xã hội
bạn thân tri kỷ đồng nghiệp đối tác khách quý người lạ người quen
trẻ em thanh niên trung niên người già phụ nữ đàn ông giới tính
tình bạn tình thân tình đồng chí tình đồng đội tình quê hương
nụ cười nước mắt giọt mồ hôi giọt sương ánh nắng cơn mưa cầu vồng
bình minh hoàng hôn đêm khuya rạng sáng chiều tà sớm mai tối muộn
hôm nay hôm qua ngày mai tuần tháng năm thập kỷ thế kỷ
đầu cuối trên dưới trong ngoài trước sau trái phải giữa cạnh bên
to nhỏ lớn bé cao thấp dài ngắn rộng hẹp sâu cạn nặng nhẹ
nhanh chậm sớm muộn gần xa đông tây nam bắc trong suốt mờ đục
sáng tối nóng lạnh khô ướt cứng mềm mịn thô ráp nhẵn bóng
thơm hôi ngọt đắng chua cay mặn nhạt bùi béo ngậy đậm đà
vui buồn sướng khổ sảng khoái mệt mỏi khỏe yếu mạnh yếu
giàu nghèo sang hèn giỏi dốt khôn dại hiền dữ thật giả
đẹp xấu hay dở đúng sai phải trái tốt xấu thiện ác
yên tĩnh ồn ào náo nhiệt vắng vẻ đông đúc chật chội rộng rãi
sạch bẩn gọn gàng bừa bộn ngăn nắp lộn xộn mới cũ xưa nay
trẻ già non chín sống chết còn mất được thua thắng bại
cho nhận vay mượn mua bán đổi trao gửi tặng biếu chúc mừng
hỏi đáp trả lời giải thích chứng minh thuyết phục thương lượng
đồng ý phản đối ủng hộ phê phán khen chê đánh giá nhận xét
học hỏi rèn luyện phấn đấu thi đua khen thưởng kỷ luật
lao động sản xuất kinh doanh buôn bán xuất nhập khẩu
xây dựng kiến thiết bảo vệ giữ gìn phát huy kế thừa
tìm kiếm khám phá phát hiện sáng chế phát minh cải tiến
bảo tồn phát triển mở rộng thu hẹp tăng giảm lên xuống
đi đến về qua lại ra vào lên xuống tới lui quanh co
ngồi đứng nằm quỳ bò chạy nhảy múa hát ca ngâm vịnh
đọc viết nghe nói nhìn ngó trông thấy quan sát theo dõi
nghĩ suy tưởng tượng mơ mộng hy vọng mong đợi chờ mong
lo lắng băn khoăn trăn trở suy tư chiêm nghiệm giác ngộ
tin tưởng nghi ngờ thắc mắc tò mò hiếu kỳ ham học
vui vẻ hồ hởi phấn khởi háo hức náo nức rộn ràng
buồn bã u sầu ủ rũ chán nản thất vọng tuyệt vọng
giận dữ bực bội cáu gắt phẫn nộ căm thù hận thù
sợ hãi lo âu hoảng hốt kinh hoàng run rẩy bối rối
ngạc nhiên bất ngờ sửng sốt kinh ngạc thán phục ngưỡng mộ
yêu mến quý trọng kính nể tôn vinh ca ngợi tự hào
ghen tị đố kỵ ích kỷ hẹp hòi tham lam keo kiệt
rộng lượng hào phóng bao dung độ lượng nhân hậu từ bi
thật thà trung thực ngay thẳng chính trực liêm khiết
gian dối lừa gạt dối trá bịp bợm xảo quyệt mưu mô
dũng cảm gan dạ kiên cường bất khuất hiên ngang anh dũng
nhút nhát rụt rè e thẹn mắc cỡ bẽn lẽn ngại ngùng
thông minh sáng dạ nhanh trí lanh lợi khôn ngoan tài giỏi
ngốc nghếch khờ dại ngớ ngẩn ngờ nghệch chậm chạp lề mề
cẩn thận tỉ mỉ chu đáo kỹ lưỡng chi tiết cụ thể
cẩu thả sơ sài qua loa đại khái ẩu đả bừa bãi
""".split()

# mở rộng lên vocab_size bằng cách ghép từ đơn thành từ ghép có nghĩa
# vd: "tình_yêu", "máy_tính" — vẫn đếm là 1 item (1 dòng = 1 từ)
import itertools as _it

def build_vocab(target):
    # giữ nguyên BASE, sau đó ghép ngẫu nhiên tạo thêm từ
    vocab = []
    seen = set()
    for w in BASE:
        w = w.strip().lower()
        if w and w not in seen:
            seen.add(w)
            vocab.append(w)
            if len(vocab) >= target:
                return vocab[:target]
    # từ ghép 2 tiếng
    random.seed(123)
    # danh sách tiếng đơn phổ biến để ghép
    syllables = list(seen)
    # một số hậu tố/tiền tố hay gặp
    while len(vocab) < target:
        a = random.choice(syllables)
        b = random.choice(syllables)
        if a == b:
            continue
        # 30% nối bằng _, còn lại nối liền cho đa dạng
        w = f"{a}_{b}" if random.random() < 0.3 else f"{a}{b}"
        if w not in seen:
            seen.add(w)
            vocab.append(w)
    return vocab[:target]

# 1. Load hoặc tạo vocab
if os.path.exists(args.vocab):
    with open(args.vocab, encoding="utf-8") as f:
        vocab = [ln.strip() for ln in f if ln.strip()]
    print(f"Loaded vocab: {len(vocab)} từ từ {args.vocab}")
    if len(vocab) < args.vocab_size:
        print(f"  vocab thiếu {args.vocab_size - len(vocab)} từ, sẽ bổ sung thêm.")
        extra = build_vocab(args.vocab_size)
        # thêm những từ chưa có
        s = set(vocab)
        for w in extra:
            if w not in s:
                vocab.append(w)
                s.add(w)
                if len(vocab) >= args.vocab_size:
                    break
        print(f"  -> vocab sau bổ sung: {len(vocab)} từ")
    else:
        vocab = vocab[:args.vocab_size]
else:
    vocab = build_vocab(args.vocab_size)
    os.makedirs(os.path.dirname(args.vocab) or ".", exist_ok=True)
    with open(args.vocab, "w", encoding="utf-8") as f:
        f.write("\n".join(vocab) + "\n")
    print(f"Đã tạo vocab {len(vocab)} từ tiếng Việt -> {args.vocab}")

n = len(vocab)

# 2. weights
try:
    import numpy as np
    has_np = True
except ImportError:
    has_np = False

if args.uniform:
    weights = None
    print("Phân bố: uniform")
else:
    if has_np:
        ranks = np.arange(1, n + 1, dtype=np.float64)
        weights = 1.0 / np.power(ranks, args.zipf_s)
        weights /= weights.sum()
        print(f"Phân bố: Zipf s={args.zipf_s} (numpy), top 5 weight: {weights[:5]}")
    else:
        weights = [1.0 / ((i + 1) ** args.zipf_s) for i in range(n)]
        s = sum(weights)
        weights = [w / s for w in weights]
        print(f"Phân bố: Zipf s={args.zipf_s} (pure python)")

os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

# 3. Gen stream
print(f"Gen {args.lines:,} dòng -> {args.output}  (batch={args.batch:,}) ...")
written = 0
with open(args.output, "w", encoding="utf-8", buffering=1024 * 1024) as out:
    if has_np and weights is not None:
        for start in range(0, args.lines, args.batch):
            cur = min(args.batch, args.lines - start)
            idx = np.random.choice(n, size=cur, p=weights)
            out.write("\n".join(vocab[i] for i in idx) + "\n")
            written += cur
            if written % 10_000_000 == 0:
                print(f"  ... {written:,}/{args.lines:,}")
    elif has_np and weights is None:
        for start in range(0, args.lines, args.batch):
            cur = min(args.batch, args.lines - start)
            idx = np.random.randint(0, n, size=cur)
            out.write("\n".join(vocab[i] for i in idx) + "\n")
            written += cur
            if written % 10_000_000 == 0:
                print(f"  ... {written:,}/{args.lines:,}")
    else:
        for start in range(0, args.lines, args.batch):
            cur = min(args.batch, args.lines - start)
            picks = random.choices(vocab, weights=weights, k=cur) if weights else random.choices(vocab, k=cur)
            out.write("\n".join(picks) + "\n")
            written += cur
            if written % 10_000_000 == 0:
                print(f"  ... {written:,}/{args.lines:,}")

size = os.path.getsize(args.output)
print(f"Xong: {written:,} dòng, {size/1024/1024:.1f} MiB, {size/1024/1024/1024:.2f} GiB")
print("Preview 10 dòng đầu:")
with open(args.output, encoding="utf-8") as f:
    for _ in range(10):
        print(" ", f.readline().rstrip())
print(f"\nĐẩy lên HDFS:\n  docker cp {args.output} namenode:/tmp/vocab-200M.txt\n  docker exec namenode hdfs dfs -put -f /tmp/vocab-200M.txt /training/top-k/input/keywords-200M-vn.txt")
