"""Merchant catalog for realistic transaction generation"""

MERCHANT_CATALOG = [
    # 식비 - 카페
    {'name': '스타벅스 강남점', 'category': '식비', 'subcategory': '카페', 'min_amount': 4000, 'max_amount': 15000},
    {'name': '이디야커피 역삼점', 'category': '식비', 'subcategory': '카페', 'min_amount': 2000, 'max_amount': 8000},
    {'name': '투썸플레이스 선릉점', 'category': '식비', 'subcategory': '카페', 'min_amount': 5000, 'max_amount': 12000},
    {'name': '커피빈 코엑스점', 'category': '식비', 'subcategory': '카페', 'min_amount': 4500, 'max_amount': 10000},
    
    # 식비 - 외식
    {'name': '맥도날드 강남점', 'category': '식비', 'subcategory': '외식', 'min_amount': 5000, 'max_amount': 15000},
    {'name': '버거킹 역삼점', 'category': '식비', 'subcategory': '외식', 'min_amount': 6000, 'max_amount': 12000},
    {'name': '롯데리아 선릉점', 'category': '식비', 'subcategory': '외식', 'min_amount': 4000, 'max_amount': 10000},
    {'name': '김밥천국 논현점', 'category': '식비', 'subcategory': '외식', 'min_amount': 3000, 'max_amount': 8000},
    {'name': '본죽 강남점', 'category': '식비', 'subcategory': '외식', 'min_amount': 5000, 'max_amount': 12000},
    {'name': '스시로 코엑스점', 'category': '식비', 'subcategory': '외식', 'min_amount': 15000, 'max_amount': 35000},
    
    # 식비 - 마트
    {'name': '이마트 월계점', 'category': '식비', 'subcategory': '마트', 'min_amount': 20000, 'max_amount': 150000},
    {'name': '홈플러스 강남점', 'category': '식비', 'subcategory': '마트', 'min_amount': 15000, 'max_amount': 120000},
    {'name': '롯데마트 서울역점', 'category': '식비', 'subcategory': '마트', 'min_amount': 18000, 'max_amount': 130000},
    {'name': 'GS25 역삼점', 'category': '식비', 'subcategory': '편의점', 'min_amount': 1000, 'max_amount': 20000},
    {'name': 'CU 선릉점', 'category': '식비', 'subcategory': '편의점', 'min_amount': 1000, 'max_amount': 18000},
    
    # 교통
    {'name': '서울교통공사', 'category': '교통', 'subcategory': '대중교통', 'min_amount': 1400, 'max_amount': 2500},
    {'name': '카카오T', 'category': '교통', 'subcategory': '택시', 'min_amount': 5000, 'max_amount': 30000},
    {'name': 'SK주유소 강남점', 'category': '교통', 'subcategory': '주유', 'min_amount': 30000, 'max_amount': 100000},
    {'name': 'GS칼텍스 역삼점', 'category': '교통', 'subcategory': '주유', 'min_amount': 30000, 'max_amount': 100000},
    
    # 쇼핑 - 의류
    {'name': '유니클로 강남점', 'category': '쇼핑', 'subcategory': '의류', 'min_amount': 20000, 'max_amount': 100000},
    {'name': 'ZARA 코엑스점', 'category': '쇼핑', 'subcategory': '의류', 'min_amount': 30000, 'max_amount': 150000},
    {'name': 'H&M 명동점', 'category': '쇼핑', 'subcategory': '의류', 'min_amount': 15000, 'max_amount': 80000},
    {'name': '무신사스토어 성수점', 'category': '쇼핑', 'subcategory': '의류', 'min_amount': 40000, 'max_amount': 200000},
    
    # 쇼핑 - 온라인
    {'name': '쿠팡', 'category': '쇼핑', 'subcategory': '온라인', 'min_amount': 10000, 'max_amount': 200000},
    {'name': '네이버페이', 'category': '쇼핑', 'subcategory': '온라인', 'min_amount': 5000, 'max_amount': 150000},
    {'name': '11번가', 'category': '쇼핑', 'subcategory': '온라인', 'min_amount': 8000, 'max_amount': 100000},
    
    # 문화
    {'name': 'CGV 강남점', 'category': '문화', 'subcategory': '영화', 'min_amount': 12000, 'max_amount': 30000},
    {'name': '메가박스 코엑스점', 'category': '문화', 'subcategory': '영화', 'min_amount': 12000, 'max_amount': 28000},
    {'name': '롯데시네마 월드타워점', 'category': '문화', 'subcategory': '영화', 'min_amount': 13000, 'max_amount': 32000},
    {'name': '교보문고 광화문점', 'category': '문화', 'subcategory': '서적', 'min_amount': 10000, 'max_amount': 50000},
    
    # 건강
    {'name': '서울대병원', 'category': '건강', 'subcategory': '병원', 'min_amount': 10000, 'max_amount': 100000},
    {'name': '연세세브란스병원', 'category': '건강', 'subcategory': '병원', 'min_amount': 15000, 'max_amount': 120000},
    {'name': '온누리약국 강남점', 'category': '건강', 'subcategory': '약국', 'min_amount': 5000, 'max_amount': 50000},
    {'name': '헬스장 역삼점', 'category': '건강', 'subcategory': '운동', 'min_amount': 50000, 'max_amount': 150000},
    
    # 통신
    {'name': 'SKT 요금', 'category': '통신', 'subcategory': '통신비', 'min_amount': 40000, 'max_amount': 80000},
    {'name': 'KT 요금', 'category': '통신', 'subcategory': '통신비', 'min_amount': 35000, 'max_amount': 75000},
    {'name': 'LG유플러스 요금', 'category': '통신', 'subcategory': '통신비', 'min_amount': 38000, 'max_amount': 70000},
    
    # 주거
    {'name': '월세', 'category': '주거', 'subcategory': '월세', 'min_amount': 500000, 'max_amount': 1500000},
    {'name': '관리비', 'category': '주거', 'subcategory': '관리비', 'min_amount': 50000, 'max_amount': 200000},
    {'name': '한국전력공사', 'category': '주거', 'subcategory': '공과금', 'min_amount': 30000, 'max_amount': 100000},
    {'name': '서울시 수도요금', 'category': '주거', 'subcategory': '공과금', 'min_amount': 20000, 'max_amount': 60000},
]


def get_merchant_catalog():
    """Get merchant catalog"""
    return MERCHANT_CATALOG


def get_merchants_by_category(category):
    """Get merchants by category"""
    return [m for m in MERCHANT_CATALOG if m['category'] == category]
