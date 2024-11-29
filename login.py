import streamlit as st
from static.main import main


# login
def user_login():
    username = st.text_input("用户名")
    password = st.text_input("密码", type="password")

    if st.button("登录"):
        if username == "root" and password == "123456":
            st.success("登录成功")
            main()

if __name__ == '__main__':
    user_login()