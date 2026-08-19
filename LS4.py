"""
Phần A:
1. Dữ liệu Form cần nhận
    form data: full_name (string), email (string, regex validation), phone (string), position_id (string/int)
    file: cv_file, PDF, DOCX, avatar_file, JPG, PNG

2. Quy tắc kiểm tra
    định dạng file
    kích thước file
    hồ sơ đã tồn tại chưa
    tên file

3. Kết quả
    201 Created: Cả 2 file lưu thành công, bản ghi DB được tạo
    400 Bad Request: Form thiếu trường, sai định dạng email, sai định dạng file 
    409 Conflict: Ứng viên đã nộp hồ sơ vào vị trí này trước đó
    413 Payload Too Large: Dung lượng CV vượt quá 10 MB hoặc Avatar vượt quá 3 MB
    500 Internal Server Error: Lỗi ghi đĩa I/O hoặc lỗi giao dịch cơ sở dữ liệu

4. Trạng thái hệ thống khi lỗi
    Mọi file tạm/chính vừa tạo trên disk bị xóa sạch
    Transaction trong cơ sở dữ liệu bị ROLLBACK
    Không có file rác (orphan files) hay bản ghi mồ côi tồn tại

Phần B:
    Giải pháp 1 (Đọc toàn bộ - In-memory Buffer): content = await file.read() nạp toàn bộ file vào RAM trước khi ghi ra đĩa. Phù hợp file siêu nhỏ (< 500 KB), nhưng nguy cơ sập RAM (OOM) khi có nhiều request đồng thời
    Giải pháp 2 (Ghi trực tiếp theo Chunk): Đọc từng block (vd: 1 MB) và ghi trực tiếp vào đường dẫn đích. Kiểm soát RAM tốt (chỉ tốn 1 MB * số connection), nhưng khó rollback nếu luồng xử lý bị crash giữa chừng
    Giải pháp 3 (Ghi Chunk vào Thư mục Tạm + Atomic Move): Ghi chunk qua bộ đệm tạm (/tmp/uploads/), kiểm tra tính toàn vẹn và magic number. Khi tất cả file hợp lệ và DB commit thành công, thực hiện lệnh os.replace (atomic move) sang thư mục lưu trữ chính thức

Phần C:
    Đọc toàn bộ vào RAM (P1): Đơn giản nhất nhưng rủi ro cao nhất. Khi nhiều người upload đồng thời, server dễ bị tràn bộ nhớ (OOM) do phải nạp toàn bộ file vào RAM trước khi kiểm tra dung lượng
    Ghi theo Chunk trực tiếp (P2): Tiết kiệm RAM tối đa (chỉ tốn dung lượng theo từng block 1 MB) và ngắt kết nối ngay khi file vượt ngưỡng, nhưng khó dọn dẹp sạch sẽ nếu quá trình ghi bị ngắt quãng giữa chừng
    Ghi Chunk vào file tạm + Chuyển file Atomic (P3): Kết hợp khả năng kiểm soát RAM của P2 với cơ chế cách ly an toàn. Nếu xảy ra lỗi ở bất kỳ bước nào, toàn bộ file tạm sẽ bị xóa ngay lập tức mà không ảnh hưởng đến thư mục lưu trữ chính

    Lựa chọn tối ưu & Đánh đổi:
        Phương án chọn: P3 (File tạm + Chunk) vì giải quyết triệt để vấn đề nghẽn bộ nhớ và đảm bảo tính toàn vẹn dữ liệu (không tạo file rác khi một trong hai file bị lỗi)
        Đánh đổi: Tốn thêm thao tác I/O để di chuyển file (atomic move/rename) và yêu cầu thư mục tạm cùng thư mục đích phải nằm trên cùng một phân vùng ổ cứng

"""

import os
import uuid
import aiofiles
import filetype
from typing import Set
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, status
from pydantic import EmailStr

app = FastAPI()

TEMP_DIR = "/app/data/temp"
PERM_DIR = "/app/data/storage"
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(PERM_DIR, exist_ok=True)

CHUNK_SIZE = 1024 * 1024  # 1 MB

ALLOWED_CV_MIMES = {"application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
ALLOWED_IMG_MIMES = {"image/jpeg", "image/png"}

MOCK_DB_CANDIDATES = set()

async def save_and_validate_file_stream(
    upload_file: UploadFile,
    max_size: int,
    allowed_mimes: Set[str],
    temp_filename: str
) -> str:
    temp_path = os.path.join(TEMP_DIR, temp_filename)
    total_size = 0
    first_chunk = True

    try:
        async with aiofiles.open(temp_path, "wb") as f:
            while chunk := await upload_file.read(CHUNK_SIZE):
                total_size += len(chunk)
                if total_size > max_size:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"File {upload_file.filename} exceeds limit of {max_size // (1024*1024)}MB."
                    )
                
                if first_chunk:
                    kind = filetype.guess(chunk)
                    mime = kind.mime if kind else upload_file.content_type
                    if mime not in allowed_mimes:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Invalid file type for {upload_file.filename}. Detected: {mime}"
                        )
                    first_chunk = False

                await f.write(chunk)
                
        return temp_path
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise

@app.post("/api/v1/applications", status_code=status.HTTP_201_CREATED)
async def submit_application(
    full_name: str = Form(...),
    email: EmailStr = Form(...),
    phone: str = Form(...),
    position_id: str = Form(...),
    cv: UploadFile = File(...),
    avatar: UploadFile = File(...)
):

    candidate_key = (email, position_id)
    if candidate_key in MOCK_DB_CANDIDATES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="You have already applied for this position."
        )

    cv_temp_id = f"cv_{uuid.uuid4()}"
    avatar_temp_id = f"avatar_{uuid.uuid4()}"
    
    saved_temp_files = []

    try:
        cv_temp_path = await save_and_validate_file_stream(
            upload_file=cv,
            max_size=10 * 1024 * 1024,
            allowed_mimes=ALLOWED_CV_MIMES,
            temp_filename=cv_temp_id
        )
        saved_temp_files.append(cv_temp_path)

        avatar_temp_path = await save_and_validate_file_stream(
            upload_file=avatar,
            max_size=3 * 1024 * 1024,
            allowed_mimes=ALLOWED_IMG_MIMES,
            temp_filename=avatar_temp_id
        )
        saved_temp_files.append(avatar_temp_path)

        cv_ext = os.path.splitext(cv.filename)[1]
        avatar_ext = os.path.splitext(avatar.filename)[1]
        
        cv_perm_path = os.path.join(PERM_DIR, f"{cv_temp_id}{cv_ext}")
        avatar_perm_path = os.path.join(PERM_DIR, f"{avatar_temp_id}{avatar_ext}")

        os.replace(cv_temp_path, cv_perm_path)
        os.replace(avatar_temp_path, avatar_perm_path)

        MOCK_DB_CANDIDATES.add(candidate_key)

        return {
            "status": "success",
            "data": {
                "full_name": full_name,
                "email": email,
                "cv_path": cv_perm_path,
                "avatar_path": avatar_perm_path
            }
        }

    except Exception as e:
        for path in saved_temp_files:
            if os.path.exists(path):
                os.remove(path)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal processing error: {str(e)}"
        )