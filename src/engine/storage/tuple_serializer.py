import struct

class TupleSerializer:
    def __init__(self, schema):
        self.schema = schema
        self.format_string = self._get_format_string(schema)
        self.record_size = struct.calcsize(self.format_string)

    def serialize(self, row):
        format_string = self.format_string
        schema = self.schema
        packed_data = []
        for i, val in enumerate(row):
            if hasattr(schema[i], 'data_type'):
                col_type = schema[i].data_type.upper()
            else:
                col_type = schema[i][1].upper()
            # None值处理
            if val is None:
                if col_type in ['INT', 'INTEGER'] or col_type == 'int':
                    raise ValueError(f"INT类型字段不允许为None，第{i}列")
                else:
                    if col_type.startswith('STR'):
                        length = int(col_type[4:-1])
                    elif col_type in ['VARCHAR', 'CHAR', 'TEXT']:
                        length = 50
                    elif col_type in ['DECIMAL', 'FLOAT', 'DOUBLE']:
                        length = 20
                    else:
                        length = 50
                    packed_data.append(b'\x00' * length)
                    continue
            # 处理DECIMAL类型 - 转换为数值
            if col_type.startswith('DECIMAL') or col_type.startswith('FLOAT') or col_type.startswith('DOUBLE'):
                if isinstance(val, str):
                    clean_val = val
                    if clean_val.startswith("'") and clean_val.endswith("'"):
                        clean_val = clean_val[1:-1]
                    elif clean_val.startswith('"') and clean_val.endswith('"'):
                        clean_val = clean_val[1:-1]
                    try:
                        # 将字符串转换为浮点数
                        float_val = float(clean_val)
                        val_str = str(float_val)
                    except ValueError:
                        val_str = clean_val
                else:
                    val_str = str(float(val))
                
                # 处理带长度的数据类型
                if '(' in col_type and ')' in col_type:
                    # 提取类型名和长度，如 DECIMAL(10,2) -> DECIMAL, 10
                    type_name = col_type.split('(')[0]
                    length_str = col_type.split('(')[1].split(')')[0]
                    if ',' in length_str:
                        # DECIMAL(10,2) 格式，取第一个数字
                        length = int(length_str.split(',')[0])
                    else:
                        length = int(length_str)
                else:
                    length = 20
                
                # 对于DECIMAL类型，长度应该是字节长度，不是数字位数
                if col_type.startswith('DECIMAL'):
                    # DECIMAL类型需要足够的字节来存储数字字符串
                    # 估算需要的字节数：数字位数 + 小数点 + 可能的负号
                    if '(' in col_type and ')' in col_type:
                        precision_str = col_type.split('(')[1].split(')')[0]
                        if ',' in precision_str:
                            total_digits = int(precision_str.split(',')[0])
                            # 为DECIMAL分配足够的字节：总位数 + 小数点 + 可能的负号
                            length = total_digits + 2  # +2 for decimal point and potential negative sign
                
                # 对于DECIMAL类型，确保精度正确
                if col_type.startswith('DECIMAL') and '(' in col_type and ')' in col_type:
                    # 提取精度信息，如 DECIMAL(3,2) -> 总长度3，小数位2
                    precision_str = col_type.split('(')[1].split(')')[0]
                    if ',' in precision_str:
                        total_digits = int(precision_str.split(',')[0])
                        decimal_places = int(precision_str.split(',')[1])
                        # 确保值不超过精度限制
                        try:
                            float_val = float(val_str)
                            # 检查整数部分是否超出限制
                            integer_part = int(float_val)
                            max_integer_digits = total_digits - decimal_places
                            if len(str(integer_part)) > max_integer_digits:
                                # 如果超出，截断到允许的范围
                                max_integer = 10 ** max_integer_digits - 1
                                integer_part = min(integer_part, max_integer)
                                # 保持小数部分，但限制总精度
                                decimal_part = float_val - int(float_val)
                                float_val = float(integer_part) + decimal_part
                                # 四舍五入到指定的小数位数
                                float_val = round(float_val, decimal_places)
                                val_str = str(float_val)
                        except ValueError:
                            pass
                
                val_bytes = val_str.encode('utf-8')
                
                if len(val_bytes) > length:
                    val_bytes = val_bytes[:length]
                else:
                    val_bytes = val_bytes.ljust(length, b'\x00')
                packed_data.append(val_bytes)
            elif col_type.startswith('VARCHAR') or col_type.startswith('CHAR') or col_type.startswith('TEXT') or col_type.startswith('TIMESTAMP') or col_type.startswith('STR'):
                if isinstance(val, str):
                    clean_val = val
                    if clean_val.startswith("'") and clean_val.endswith("'"):
                        clean_val = clean_val[1:-1]
                    elif clean_val.startswith('"') and clean_val.endswith('"'):
                        clean_val = clean_val[1:-1]
                    val_bytes = clean_val.encode('utf-8')
                else:
                    val_bytes = str(val).encode('utf-8')
                
                # 处理带长度的数据类型
                if '(' in col_type and ')' in col_type:
                    # 提取类型名和长度，如 VARCHAR(100) -> VARCHAR, 100
                    type_name = col_type.split('(')[0]
                    length_str = col_type.split('(')[1].split(')')[0]
                    if ',' in length_str:
                        # DECIMAL(10,2) 格式，取第一个数字
                        length = int(length_str.split(',')[0])
                    else:
                        length = int(length_str)
                else:
                    type_name = col_type
                    if type_name.startswith('STR'):
                        print(f"[DEBUG] type_name = {type_name}, type_name[4:-1] = '{type_name[4:-1]}'")
                        length = int(type_name[4:-1])
                    elif type_name in ['VARCHAR', 'CHAR', 'TEXT']:
                        length = 50
                    elif type_name in ['DECIMAL', 'FLOAT', 'DOUBLE']:
                        length = 20
                    else:
                        length = 50
                
                if len(val_bytes) > length:
                    val_bytes = val_bytes[:length]
                else:
                    val_bytes = val_bytes.ljust(length, b'\x00')
                packed_data.append(val_bytes)
            elif col_type in ['DATE', 'TIME', 'TIMESTAMP']:
                val_str = str(val)
                val_bytes = val_str.encode('utf-8')
                length = 20
                if len(val_bytes) > length:
                    val_bytes = val_bytes[:length]
                else:
                    val_bytes = val_bytes.ljust(length, b'\x00')
                packed_data.append(val_bytes)
            else:
                if isinstance(val, str):
                    try:
                        packed_data.append(int(float(val)))
                    except ValueError:
                        packed_data.append(0)
                else:
                    packed_data.append(val)
        try:
            return struct.pack(format_string, *packed_data)
        except struct.error as e:
            print(f"Debug: format_string = {format_string}")
            print(f"Debug: packed_data = {packed_data}")
            print(f"Debug: data types = {[type(x) for x in packed_data]}")
            print(f"Debug: original row = {row}")
            print(f"Debug: schema = {schema}")
            print(f"Debug: packed_data details:")
            for i, item in enumerate(packed_data):
                print(f"  [{i}] {item} (type: {type(item)})")
            raise e

    def deserialize(self, row_data):
        format_string = self.format_string
        schema = self.schema
        unpacked_data = struct.unpack(format_string, row_data)
        processed_data = []
        for i, val in enumerate(unpacked_data):
            if hasattr(schema[i], 'data_type'):
                col_type = schema[i].data_type.upper()
            else:
                col_type = schema[i][1].upper()
            # 处理字符串类型
            if col_type.startswith('VARCHAR') or col_type.startswith('CHAR') or col_type.startswith('TEXT') or col_type.startswith('STR'):
                decoded = val.decode('utf-8').rstrip('\x00')
                if decoded.startswith("b'") and decoded.endswith("'"):
                    inner = decoded[2:-1]
                    inner = inner.replace('\\x00', '').replace('\\n', '\n').replace('\\t', '\t')
                    if inner.startswith('"') and inner.endswith('"'):
                        inner = inner[1:-1]
                    elif inner.startswith("'") and inner.endswith("'"):
                        inner = inner[1:-1]
                    decoded = inner
                elif decoded.startswith("b'") and '\\x' in decoded:
                    inner = decoded[2:-1]
                    import codecs
                    try:
                        inner = codecs.decode(inner, 'unicode_escape')
                    except:
                        pass
                    if inner.startswith('"') and inner.endswith('"'):
                        inner = inner[1:-1]
                    elif inner.startswith("'") and inner.endswith("'"):
                        inner = inner[1:-1]
                    decoded = inner
                elif decoded.startswith("'") and decoded.endswith("'"):
                    decoded = decoded[1:-1]
                elif decoded.startswith('"') and decoded.endswith('"'):
                    decoded = decoded[1:-1]
                processed_data.append(decoded)
            # 处理数值类型
            elif col_type.startswith('DECIMAL') or col_type.startswith('FLOAT') or col_type.startswith('DOUBLE'):
                decoded = val.decode('utf-8').rstrip('\x00')
                try:
                    # 尝试转换为浮点数
                    float_val = float(decoded)
                    processed_data.append(float_val)
                except ValueError:
                    # 如果转换失败，保持为字符串
                    processed_data.append(decoded)
            # 处理时间类型
            elif col_type in ['TIMESTAMP'] or col_type.startswith('TIMESTAMP'):
                decoded = val.decode('utf-8').rstrip('\x00')
                processed_data.append(decoded)
            else:
                processed_data.append(val)
        return tuple(processed_data)

    def get_record_size(self):
        return self.record_size

    def _get_format_string(self, schema):
        format_string = ''
        for col in schema:
            if hasattr(col, 'data_type'):
                col_type = col.data_type.upper()
            else:
                col_type = col[1].upper()
            
            # 处理带长度的数据类型
            if '(' in col_type and ')' in col_type:
                # 提取类型名和长度，如 VARCHAR(100) -> VARCHAR, 100
                type_name = col_type.split('(')[0]
                length_str = col_type.split('(')[1].split(')')[0]
                if ',' in length_str:
                    # DECIMAL(10,2) 格式，取第一个数字
                    length = int(length_str.split(',')[0])
                else:
                    length = int(length_str)
            else:
                type_name = col_type
                length = None
            
            # 对于DECIMAL类型，长度应该是字节长度，不是数字位数
            if col_type.startswith('DECIMAL'):
                # DECIMAL类型需要足够的字节来存储数字字符串
                if '(' in col_type and ')' in col_type:
                    precision_str = col_type.split('(')[1].split(')')[0]
                    if ',' in precision_str:
                        total_digits = int(precision_str.split(',')[0])
                        # 为DECIMAL分配足够的字节：总位数 + 小数点 + 可能的负号
                        length = total_digits + 2  # +2 for decimal point and potential negative sign
            
            if type_name in ['INT', 'INTEGER'] or type_name == 'int':
                format_string += 'i'
            elif type_name in ['VARCHAR', 'CHAR', 'TEXT'] or type_name.startswith('STR'):
                if length is None:
                    length = 50
                format_string += f'{length}s'
            elif type_name in ['DECIMAL', 'FLOAT', 'DOUBLE', 'TIMESTAMP']:
                if length is None:
                    length = 20
                format_string += f'{length}s'
            elif type_name in ['DATE', 'TIME']:
                if length is None:
                    length = 20
                format_string += f'{length}s'
            else:
                length = 50
                format_string += f'{length}s'
        return format_string