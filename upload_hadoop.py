import os
import subprocess
from datetime import datetime

# Đường dẫn thư mục chứa JSON gốc
source_folder = r"D:\Booking Hotel Scraping\Booking-hotel-collection\hotel_data"

# Đường dẫn Data Lake trên HDFS
hdfs_root = "/data_lake/raw/hotels"

# Hàm kiểm tra và tạo thư mục trên HDFS nếu chưa tồn tại
def ensure_hdfs_directory(hdfs_path):
    try:
        print(f"Checking if HDFS directory exists: {hdfs_path}")
        check_cmd = ["hdfs.cmd", "dfs", "-ls", hdfs_path]
        result = subprocess.run(check_cmd, capture_output=True, text=True)

        if "No such file or directory" in result.stderr or result.returncode != 0:
            print(f"Creating HDFS directory: {hdfs_path}")
            mkdir_cmd = ["hdfs.cmd", "dfs", "-mkdir", "-p", hdfs_path]
            subprocess.run(mkdir_cmd, capture_output=True, text=True, check=True)
            print(f"Created HDFS directory: {hdfs_path}")
        else:
            print(f"HDFS directory {hdfs_path} already exists.")
    except subprocess.CalledProcessError as e:
        print(f"Error creating HDFS directory {hdfs_path}: {e.stderr}")
        raise

# Hàm upload cả thư mục lên HDFS
def upload_folder_to_hdfs(local_folder, hdfs_folder):
    if not os.path.exists(local_folder):
        raise FileNotFoundError(f"Local folder not found: {local_folder}")

    # Đảm bảo thư mục HDFS tồn tại
    ensure_hdfs_directory(hdfs_folder)

    print(f"Uploading folder: {local_folder} -> {hdfs_folder}")
    try:
        # Xóa thư mục cũ trên HDFS nếu đã tồn tại
        rm_cmd = ["hdfs.cmd", "dfs", "-rm", "-r", hdfs_folder]
        subprocess.run(rm_cmd, capture_output=True, text=True)

        # Upload toàn bộ thư mục
        put_cmd = ["hdfs.cmd", "dfs", "-put", local_folder, hdfs_folder]
        subprocess.run(put_cmd, capture_output=True, text=True, check=True)

        print(f"Successfully uploaded folder: {hdfs_folder}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading folder: {e.stderr}")
        raise

# Kiểm tra kết nối HDFS trước khi bắt đầu
try:
    print("Checking HDFS connection...")
    subprocess.run(["hdfs.cmd", "dfs", "-ls", "/"], capture_output=True, text=True, check=True)
    print("HDFS is accessible.")
except subprocess.CalledProcessError as e:
    print(f"Error: Cannot connect to HDFS. Ensure Hadoop is running. Error: {e.stderr}")
    exit(1)

# Gọi hàm upload thư mục
upload_folder_to_hdfs(source_folder, hdfs_root)

print("All files successfully uploaded to HDFS!")
