# -*- coding: utf-8 -*-
"""
语法分析器适配器 - 将新语法分析器适配到现有系统
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from .new_syntax_analyzer import NewSyntaxAnalyzer
from .simple_ast_converter import SimpleASTConverter


class SyntaxAdapter:
    """语法分析器适配器 - 统一新旧语法分析器接口"""
    
    def __init__(self, use_new_analyzer=True):
        self.use_new_analyzer = use_new_analyzer
        # 只使用新语法分析器
        self.analyzer = NewSyntaxAnalyzer()
        self.converter = SimpleASTConverter()
    
    def build_ast_from_tokens(self, tokens):
        """从token流构建AST"""
        # 过滤注释token，避免解析器将 COMMENT 视为语句起始
        def _is_comment(tok):
            try:
                # 元组形式: (type, value, line, col)
                if isinstance(tok, tuple) and len(tok) >= 1 and tok[0] == 'COMMENT':
                    return True
                # 对象形式: tok.type == 'COMMENT'
                if hasattr(tok, 'type') and getattr(tok, 'type') == 'COMMENT':
                    return True
            except Exception:
                return False
            return False

        filtered = [t for t in tokens or [] if not _is_comment(t)]

        # 仅包含注释或空输入
        if not filtered:
            raise ValueError("空语句或仅包含注释")

        # 检查是否需要添加分号
        if not self._has_semicolon(filtered):
            filtered = filtered + [(';', ';', 0, 0)]
        
        new_ast = self.analyzer.build_ast_from_tokens(filtered)
        
        # 使用转换器将新分析器的AST转换为兼容格式
        return self.converter.convert(new_ast)
    
    def _has_semicolon(self, tokens):
        """检查token流是否以分号结尾"""
        if not tokens:
            return False
        
        last_token = tokens[-1]
        if isinstance(last_token, tuple):
            return last_token[0] == ';' or last_token[1] == ';'
        else:
            return last_token.type == ';' or last_token.value == ';'
    
    def analyze_tokens(self, tokens, debug=False):
        """分析token流（兼容旧接口）"""
        new_ast = self.analyzer.build_ast_from_tokens(tokens)
        return self.converter.convert(new_ast)
