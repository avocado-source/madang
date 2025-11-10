import streamlit as st
import duckdb
import pandas as pd
import time

conn = duckdb.connect("madang.duckdb")

conn.execute("""
CREATE TABLE IF NOT EXISTS Customer (
    custid INTEGER PRIMARY KEY,
    name VARCHAR,
    address VARCHAR,
    phone VARCHAR
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS Book (
    bookid INTEGER PRIMARY KEY,
    bookname VARCHAR,
    publisher VARCHAR,
    price INTEGER
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS Orders (
    orderid INTEGER PRIMARY KEY,
    custid INTEGER,
    bookid INTEGER,
    saleprice INTEGER,
    orderdate DATE
);
""")

check_df = conn.execute("SELECT COUNT(*) as count FROM Book").fetchdf()
is_empty = check_df['count'][0] == 0

if is_empty:
    try:
        conn.execute("""
            INSERT INTO Book VALUES
                (1, '축구의 역사', '굿스포츠', 7000),
                (2, '축구아는 여자', '나무수', 13000),
                (3, '축구의 이해', '대한미디어', 22000),
                (4, '골프 바이블', '대한미디어', 35000),
                (5, '피겨 교본', '굿스포츠', 8000),
                (6, '역도 단계별기술', '굿스포츠', 6000),
                (7, '야구의 추억', '이상미디어', 20000),
                (8, '야구를 부탁해', '이상미디어', 13000),
                (9, '올림픽 이야기', '삼성당', 7500),
                (10, 'Olympic Champions', 'Pearson', 13000);
        """)
        
        conn.execute("""
            INSERT INTO Customer VALUES
                (1, '박지성', '영국 맨체스타', '000-5000-0001'),
                (2, '김연아', '대한민국 서울', '000-6000-0001'),
                (3, '장미란', '대한민국 강원도', '000-7000-0001'),
                (4, '추신수', '미국 클리블랜드', '000-8000-0001'),
                (5, '박세리', '대한민국 대전', NULL),
                (6, '한지예', '인천광역시', '010-0000-0000');
        """)

        conn.execute("""
            INSERT INTO Orders VALUES
                (1, 1, 1, 6000, '2014-07-01'),
                (2, 1, 3, 21000, '2014-07-03'),
                (3, 2, 5, 8000, '2014-07-03'),
                (4, 3, 6, 6000, '2014-07-04'),
                (5, 4, 7, 20000, '2014-07-05'),
                (6, 1, 2, 12000, '2014-07-07'),
                (7, 4, 8, 13000, '2014-07-07'),
                (8, 3, 10, 12000, '2014-07-08'),
                (9, 2, 10, 7000, '2014-07-09'),
                (10, 3, 8, 13000, '2014-07-10');
        """)
        st.success("원본 '마당' 데이터베이스를 성공적으로 입력했습니다. 앱을 새로고침합니다.")
        time.sleep(2) 
        st.rerun()

    except Exception as e:
        st.error(f"데이터 삽입 중 오류 발생: {e}")

def query(sql):
    return conn.execute(sql).fetchdf()

books = [None]
result = query("SELECT bookid, bookname FROM Book;")
for _, row in result.iterrows():
    books.append(f"{row['bookid']},{row['bookname']}")

st.title("마당서점 관리 시스템")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "고객조회",
    "거래 입력",
    "도서 구매내역",
    "베스트셀러",
    "신규 도서등록"
])

name = tab1.text_input("고객명")
custid = None

def render_transaction_tab(customer_id, customer_name):
    tab2.write(f"고객번호: {customer_id}")
    tab2.write(f"고객명: {customer_name}")
    
    select_book = tab2.selectbox("구매 서적:", books, key=f"selectbox_{customer_id}", index=0) 
    if select_book is not None:
        bookid = select_book.split(",")[0]
        dt = time.strftime('%Y-%m-%d', time.localtime())
        
        orderid_df = query("SELECT COALESCE(MAX(orderid),0) AS max_order FROM Orders;")
        orderid = orderid_df['max_order'][0] + 1
        
        price = tab2.text_input("금액", key=f"price_{customer_id}")
        
        if tab2.button('거래 입력', key=f"button_{customer_id}"):
            if price:
                try:
                    conn.execute(f"""
                        INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate)
                        VALUES ({orderid}, {customer_id}, {bookid}, {price}, '{dt}');
                    """)
                    tab2.success("거래가 입력되었습니다.")
                except Exception as e:
                    tab2.error(f"거래 입력 실패: {e}")
            else:
                tab2.error("금액을 입력해야 합니다.") 

if name:
    customer_df = query(f"SELECT * FROM Customer WHERE name = '{name}';")
    if not customer_df.empty:
        custid = int(customer_df['custid'][0])

        order_df = query(f"""
            SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice
            FROM Customer c
            JOIN Orders o ON c.custid = o.custid
            JOIN Book b ON o.bookid = b.bookid
            WHERE c.name = '{name}';
        """)
        if not order_df.empty:
            tab1.write(order_df)
        else:
            tab1.write("아직 주문 내역이 없습니다.")
        
        render_transaction_tab(custid, name)
    else:
        tab1.write(f"'{name}' 고객의 주문 내역이 없습니다.")
        tab1.write(f"'{name}' 고객을 추가하시겠습니까?")
        if tab1.button("추가"):
            max_id_df = query("SELECT COALESCE(MAX(custid),0) AS max_id FROM Customer;")
            new_custid = max_id_df['max_id'][0] + 1
            try:
                conn.execute(f"INSERT INTO Customer (custid, name) VALUES ({new_custid}, '{name}');")
                tab1.success(f"'{name}' (고객번호: {new_custid})님이 추가되었습니다.")
                render_transaction_tab(new_custid, name)
            except Exception as e:
                tab1.error(f"고객 추가 실패: {e}")

with tab3:
    st.subheader("도서별 구매내역")
    book_df_tab3 = query("SELECT bookid, bookname FROM Book;")
    
    if not book_df_tab3.empty:
        selected_book_name = st.selectbox(
            "책 선택:", 
            book_df_tab3['bookname'], 
            key="tab3_select",
            index=None, 
            placeholder="책을 선택하세요"
        )
        
        if selected_book_name:
            selected_book_id = int(book_df_tab3[book_df_tab3['bookname']==selected_book_name]['bookid'].iloc[0])
            st.write(f"도서 ID: {selected_book_id}")
            
            result_df = query(f"""
                SELECT DISTINCT C.name, C.phone
                FROM Orders O
                JOIN Customer C ON O.custid = C.custid
                WHERE O.bookid = {selected_book_id}
                ORDER BY C.name;
            """)
            
            if not result_df.empty:
                st.write(f"[{selected_book_name}] 구매 고객 목록")
                result_df.index = range(1, len(result_df)+1)
                st.dataframe(result_df, use_container_width=True)
            else:
                st.write("구매한 고객이 없습니다.")
    else:
        st.write("먼저 도서를 등록해주세요.")

with tab4:
    st.subheader("베스트셀러 Top 10")
    bestseller_df = query("""
        SELECT 
            B.bookname AS "책 제목",
            B.publisher AS "출판사",
            COUNT(O.bookid) AS "총 판매 수"
        FROM Orders O
        JOIN Book B ON O.bookid = B.bookid
        GROUP BY O.bookid, B.bookname, B.publisher
        ORDER BY "총 판매 수" DESC
        LIMIT 10;
    """)
    if not bestseller_df.empty:
        bestseller_df.index = range(1, len(bestseller_df)+1)
        st.dataframe(bestseller_df, use_container_width=True)
    else:
        st.write("주문 내역이 없습니다.")

with tab5:
    st.subheader("신규 도서 등록")
    max_id_df = query("SELECT COALESCE(MAX(bookid),0) AS max_id FROM Book;")
    new_id = int(max_id_df['max_id'][0] + 1)
    
    with st.form(key="new_book_form"):
        st.text_input("도서 ID", value=new_id, disabled=True)
        new_bookname = st.text_input("책 제목")
        new_publisher = st.text_input("출판사")
        new_price = st.number_input("가격", min_value=0, step=1000)
        
        submit_button = st.form_submit_button("신규 도서 입력")

    if submit_button:
        if not new_bookname or not new_publisher:
            st.error("책 제목과 출판사를 입력해야 합니다.")
        else:
            try:
                conn.execute(
                    "INSERT INTO Book (bookid, bookname, publisher, price) VALUES (?, ?, ?, ?);",
                    [new_id, new_bookname, new_publisher, new_price]
                )
                st.success("새로운 책이 등록되었습니다.")
            except Exception as e:
                st.error(f"등록 실패: {e}")