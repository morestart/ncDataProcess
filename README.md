# nc区域数据提取工具

## 依赖安装

`pip install -r requirements.txt`

## 使用说明

修改配置文件`config.yml`

eg:
```
# 数据根目录
root_path: raw_data
# 需要提取数据的经纬度
lon: [120.5, 121]
lat: [35.0, 35.5]
```
 
运行`main.py`即可
