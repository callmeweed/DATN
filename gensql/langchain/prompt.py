from langchain.prompts import PromptTemplate
from langchain.prompts import ChatPromptTemplate
from langchain_core.prompts.chat import _convert_to_message



# check SQL
checkSQL_template = """Dưới đây là câu hỏi của người dùng cùng câu SQL tương ứng truy vấn vào cơ sở dữ liệu để lấy thông tin trả lời câu hỏi:
        
- Câu hỏi: "{question}"
- Thời điểm truy vấn: {time}
- Câu truy vấn: {SQL}
- Database Schema:
```
{script_tables}
```

- Câu SQL như trên có đúng với yêu cầu trong câu hỏi không? Chỉ trả lời 'Có' hoặc 'Không' và không cần giải thích gì thêm.
- Trả lời:"""
CHECKSQL_TEMPLATE = PromptTemplate(template=checkSQL_template, input_variables=['question', 'time', 'SQL', 'script_tables'])


# generate sql
generate_sql = """Bạn là một nhà quản trị cơ sở dữ liệu. Dựa trên câu hỏi của người dùng kết hợp với thời điểm người dùng đặt câu hỏi, hãy tạo một câu SQL truy vấn vào cơ sở dữ liệu để lấy thông tin trả lời câu hỏi của người dùng.
        
# Chú ý:
- Câu SQL phải chính xác và đơn giản
- Ưu tiên sử dụng ORDER BY, rất hạn chế sử dụng MAX, MIN, GROUP BY, JOIN
- Chú ý đặc biệt đến thời điểm đặt câu hỏi, khung thời gian được đề cập trong câu hỏi
- Từ `ma`/`sma` có nghĩa là moving average/ simple moving average chứ không phải là mã cổ phiếu
- Nếu thời gian không được đề cập trong câu hỏi thì phải luôn lùi 1 khung thời gian, ví dụ: với bảng khung thời gian ngày thì lùi 1 ngày, bảng khung thời gian quý thì lùi 1 quý, bảng khung thời gian năm thì lùi 1 năm.
- Nếu thời gian được đề cập trong câu hỏi (ví dụ: năm vừa rồi, năm yyyy, phiên hôm qua, quý vừa rồi, quý x năm yyyy, phiên dd/mm, ...) thì giữ nguyên mốc thời gian trong câu hỏi.
- Nếu trường thông tin được yêu cầu trong câu hỏi của người dùng không có trong Database Schema thì trả về "Database Schema được cung cấp không có thông tin để trả lời câu hỏi", tuyệt đối không tự tạo ra công thức tính toán.
- Nếu câu hỏi bao gồm loại công ty, loại mã (ví dụ công ty bất động sản, công ty chứng khoán, công ty ngân hàng, công ty xây dựng, mã bất động sản, mã ngân hàng, mã xây dựng,...), ngành của các mã chứng khoán, lĩnh vực hoạt động của công ty ví dụ: Tài nguyên cơ bản, bán lẻ, bao bì, tiêu dùng, dịch vụ, thực phẩm, đồ uống, y tế, ô tô, vận tải, dược phẩm, truyền thông, giải trí, lâm sản, dệt may, hoá chất, kim loại và khai thác, , bảo hiểm, dầu khí, công nghệ thông tin, môi giới chứng khoán, xây dựng, năng lượng, Bất động sản, Môi giới chứng khoán, Dịch vụ tài chính và Ngân hàng thì trả về "Database Schema được cung cấp không có thông tin để trả lời câu hỏi"

# Nhiệm vụ:
- Câu hỏi: "{question}"
- Thời điểm đặt câu hỏi: {time}
- Database Schema:
```
{script_tables}
```

# Câu SQL:"""
# GENERATE_SQL_TEMPLATE = PromptTemplate(
#     template=generate_sql, input_variables=["question", "time", "script_tables"]
# )
GENERATE_SQL_TEMPLATE = ChatPromptTemplate(
    messages = [_convert_to_message(("user", generate_sql))],
    input_variables = ["question", "time", "script_tables"]
)

# repair
repair_prompt = """#Nhiệm vụ: Sửa lại câu truy vấn SQL để trả lời câu hỏi của người dùng dựa trên lỗi đi kèm, Luôn chọn các cột dữ liệu được đề cập trong câu hỏi hoặc sử dụng để lọc dữ liệu, để sắp xếp, để tính toán, Luôn phải lọc bỏ các giá trị NULL hoặc dùng NULLS LAST, nếu so sánh số lần gấp nhau cần lưu ý các giá trị 0
        
#Chỉ dẫn:
- Luôn chọn các cột dữ liệu được đề cập trong câu hỏi hoặc sử dụng để lọc dữ liệu, để sắp xếp, để tính toán ngoài việc chỉ đưa ra mỗi mã chứng khoán, symbol
- Chú ý đến lỗi của câu SQL cũ để sửa lại câu truy vấn phù hợp với schema và câu hỏi
- Luôn phải lọc bỏ các giá trị NULL, sử dụng NULLS LAST
- Chú ý đặc biệt đến thời điểm truy vấn
- Chỉ lấy các trường dữ liệu trong bảng cung cấp. Nếu bạn không thể trả lời câu hỏi với cấu trúc cơ sở dữ liệu hiện có, không chắc chắn với kết quả của mình, hãy trả về 'Tôi không biết'
- Nếu thời gian không được đề cập trong câu hỏi thì phải luôn lùi 1 khung thời gian, ví dụ: với bảng khung thời gian ngày thì lùi 1 ngày, bảng khung thời gian quý thì lùi 1 quý, bảng khung thời gian năm thì lùi 1 năm.
- Nếu thời gian được đề cập trong câu hỏi (ví dụ: năm vừa rồi, năm yyyy, phiên hôm qua, quý vừa rồi, quý x năm yyyy, phiên dd/mm, ...) thì giữ nguyên mốc thời gian trong câu hỏi.


#Câu hỏi thực tế
- Câu hỏi: {question}
- Thời điểm truy vấn: {time}
- Truy vấn cũ: {old_SQL}
- Lỗi trả về của SQL cũ: {explanation}
- Database Schema: {script_tables}
- Câu trả lời:
"""
REPAIR_TEMPLATE = PromptTemplate(
    template=repair_prompt, input_variables=["question", "time", "old_SQL", "explanation", "script_tables"]
)
