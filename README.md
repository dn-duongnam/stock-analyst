# Dự án
Tự động hóa quy trình thu thập và phân tích Dữ liệu Chứng khoán với Airow, PostgreSQL, Docker
## Mô tả
Dự án này tập trung vào việc tự động hóa quá trình thu thập dữ liệu chứng khoán và sử dụng dữ liệu đó để huấn luyện một mô hình học sâu. Mục tiêu là xây dựng một mô hình có thể dự đoán giá cổ phiếu trong tương lai dựa trên các thông tin thị trường có sẵn.
## Workflow
1. Crawl dữ liệu: Sử dụng thư viện vnstock của python

2. Insert dữ liệu vào cơ sở dữ liệu: Sử dụng hệ quản trị cơ sở dữ liệu PostgreSQL
3. Lấy dữ liệu từ cơ sở dữ liệu: Sử dụng các truy vấn SQL truy vấn dữ liệu từ cơ sở dữ liệu

4. Huấn luyện mô hình học sâu: Sử dụng TensorFlow thiết kế và huấn luyện một mô hình học sâu để dự đoán giá cổ phiếu

5. Đánh giá và kiểm thử: Đánh giá hiệu suất của mô hình học sâu bằng cách sử dụng phương pháp đánh giá như sai số trung bình (mean squared error). Kiểm thử mô hình với dữ liệu mới để xem mô hình có thể dự đoán đúng không.

6. Triển khai mô hình: Nếu mô hình có hiệu suất tốt, triển khai nó vào một môi trường sản phẩm hoặc sử dụng nó để dự đoán giá cổ phiếu trực tiếp.

## Cài đặt
Bước 1: git clone https://github.com/your-username/your-repo.git

Bước 2: docker-compose up
