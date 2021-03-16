
#总步骤

    1、利用API头文件，生成数据结构和函数
    2、使用pybind11封装pyd或so


# 生成数据结构等映射py文件

    1.生成数据类型
    python generate_data_type.py
    => 产生两个文件: ctp_typedef.py, ctp_constant.py
    
    2.生成数据结构
    python generate_struct.py
    => 产生文件: ctp_struct.py
    
    3.生成接口
    python generate_api_functions.py
    => 产生 ctp_md_*.cpp ctp_md_*.h, ctp_td_*.cpp, ctp_td_*.h

# 6.3.15 升级6.3.19
    
    1.更新覆盖 ctp/include/ctp目录下的*.h文件
    2.更新覆盖 ctp/libs目录下的 *.lib文件
    3.更新覆盖 ctp目录下的 *.dll, *.so文件
    4.在ctp/generator目录下，重新生成数据结构等映射文件
    5.更新覆盖ctp目录下的 ctp_constant.py    
    6.对比ctp/generator目录下的 ctp_md_*.h 文件，与 vnctp/vnctpmd/vnctpmd.h文件，增量更新常量、数据结构和函数名等代码
    7.对比ctp/generator目录下的 ctp_md_*.cpp 文件，与 vnctp/vnctpmd/vnctpmd.cpp文件，增量更新函数等
    8.对比ctp/generator目录下的 ctp_td_*.h 文件，与 vnctp/vnctptd/vnctptd.h文件，增量更新常量、数据结构和函数名等代码
    9.对比ctp/generator目录下的 ctp_td_*.cpp 文件，与 vnctp/vnctptd/vnctptd.cpp文件，增量更新函数等
    
#windows 编译

    1.vs 2019 打开vnctp/vnctp.sln解决方案文件
    2.检查两个子项目的属性：
     --头文件目录，包含python 3.7或者env目录下的include目录，ctp api的include目录，pybind11的include目录
     --lib目录，主要是ctp/libs目录， python 3.7或env下的libs目录
    3.使用release模式进行编译
 
  
 #linux编译
    1. 复制so文件到ctp根目录，并改名
       mv thostmduserapi_se.so libthostmduserapi_se.so 
       mv thosttraderapi_se.so libthosttraderapi_se.so
      在py37环境下运行
      python setup.py build
    2. 产生的so文件，复制改名至ctp/目录下
    cp build/lib.linux-x86_64-3.7/vnctpmd.cpython-37m-x86_64-linux-gnu.so  vnctpmd.so 
    cp build/lib.linux-x86_64-3.7/vnctptd.cpython-37m-x86_64-linux-gnu.so vnctptd.so

