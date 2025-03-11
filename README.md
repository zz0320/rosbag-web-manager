# ROS包数据处理与Web管理系统

[![Python版本](https://img.shields.io/badge/Python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![ROS](https://img.shields.io/badge/ROS-兼容-green.svg)](https://www.ros.org/)
[![许可证](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 项目概述

ROS包Web管理系统是一个专为双臂机器人设计的数据处理和管理平台，提供从ROS包(rosbag)数据提取、同步、转换到可视化的完整数据处理流程。系统包含一组高效的数据处理脚本和一个直观的Web界面，极大简化了机器人数据的处理和分析工作。

### 主要功能

- **数据提取**：从ROS包中提取机械臂、夹爪状态数据和相机图像
- **数据同步**：对异构数据（位置、速度、图像等）进行时间同步和对齐
- **格式转换**：生成结构化的H5数据格式，便于机器学习应用
- **数据重组**：按照机器人学习要求重组数据结构
- **动作发布**：将处理后的动作数据重新发布到ROS话题，用于仿真或重放
- **Web管理**：通过Web界面浏览、检查和管理ROS包数据

## 系统架构

本系统由三个核心处理脚本和一个Web应用组成：

1. **数据提取层**（A01_rosbag_extractor.py）
2. **数据处理层**（A02_convert_combined_fixed.py）
3. **动作发布层**（A03_publish_actions_h5.py）
4. **Web管理界面**（app.py）

## 系统要求

- **Python 3.6+**
- **ROS** (已在ROS Noetic上测试)
- **依赖库**：numpy, pandas, h5py, flask, tqdm, opencv-python
- **足够的存储空间**：处理大型ROS包需要大量临时存储空间

## 安装指南

### 安装依赖

```bash
# 基本依赖
pip install numpy pandas h5py flask tqdm opencv-python pillow matplotlib

# ROS相关依赖（需要先安装ROS）
pip install rospkg rosbag cv_bridge
```

### 获取项目

```bash
git clone <repository_url>
cd rosbag-web-manager
```

## 使用方法

### 1. 数据提取 - 从ROS包提取原始数据

```bash
python A01_rosbag_extractor.py --bag_path /path/to/your.bag --output_dir /path/to/extracted
```

**主要参数**：
- `--bag_path`: ROS包文件路径（必填）
- `--output_dir`: 输出目录（可选，默认为以时间戳命名的目录）
- `--topic_filter`: 过滤特定话题（可选）

**输出**：
- `csv/left_arm_states.csv`, `csv/right_arm_states.csv`: 机械臂关节状态
- `csv/left_gripper_states.csv`, `csv/right_gripper_states.csv`: 夹爪状态
- `csv/left_arm_actions.csv`, `csv/right_arm_actions.csv`: 机械臂控制命令
- `images/`: 相机图像（按时间戳命名）
- 以及其他传感器数据和元信息

### 2. 数据处理 - 同步和转换数据

```bash
python A02_convert_combined_fixed.py --input_dir /path/to/extracted --output_dir /path/to/processed
```

**主要参数**：
- `--input_dir`: 输入目录（A01脚本的输出）
- `--output_dir`: 输出目录
- `--max_workers`: 并行处理的工作线程数（默认为3）

**处理步骤**：
1. **数据同步**：使用最近邻方法对不同传感器数据进行时间同步
2. **H5转换**：将同步后的数据转换为H5格式
3. **数据重组**：按照机器学习应用的要求重新组织数据结构

**输出**：
- 结构化的H5文件，包含同步的关节状态、夹爪状态和相机图像
- 重组后的数据结构，便于机器学习应用

### 3. 动作发布 - 将处理后的动作数据发布到ROS话题

```bash
python A03_publish_actions_h5.py --h5_file /path/to/processed/data.h5 --rate 10
```

**主要参数**：
- `--h5_file`: H5数据文件路径
- `--rate`: 发布频率（Hz，默认为10）
- `--debug`: 启用调试输出
- `--manual`: 启用手动模式，需用户确认每次发布

**功能**：
- 从H5文件读取动作数据
- 将数据发布到相应的ROS话题
- 支持调试和手动模式

### 4. Web管理界面 - 浏览和管理ROS包数据

```bash
python app.py
```

启动后，访问 http://localhost:5000 打开Web界面。

**主要功能**：
- 浏览和管理ROS包文件
- 查看ROS包信息（如持续时间、话题列表等）
- 启动数据处理任务
- 查看和管理处理结果

## 数据流程示例

典型的完整数据处理流程如下：

```bash
# 1. 提取数据
python A01_rosbag_extractor.py --bag_path demo.bag --output_dir ./extracted_data

# 2. 处理和转换数据
python A02_convert_combined_fixed.py --input_dir ./extracted_data --output_dir ./processed_data

# 3. 发布动作数据（可选）
python A03_publish_actions_h5.py --h5_file ./processed_data/trajectory_data.h5 --rate 10

# 4. 启动Web界面
python app.py
```

## 常见问题

### 内存不足
处理大型ROS包文件可能需要大量内存。尝试以下方法：
- 增加系统交换空间
- 使用`--chunk_size`参数减小每次处理的数据量
- 关闭其他内存密集型应用

### 缺少依赖
如果遇到依赖问题，请确保安装了所有必需的Python包和ROS组件。特别是`cv_bridge`可能需要从源代码编译以匹配您的Python版本。

### 数据同步问题
如果数据同步结果不理想，可尝试调整同步参数：
- 检查传感器时间戳是否正确同步
- 调整时间窗口参数以获得更好的匹配

## 贡献

欢迎提交问题报告、功能请求或代码贡献。请遵循以下流程：

1. Fork本仓库
2. 创建功能分支 (`git checkout -b feature/your-feature`)
3. 提交更改 (`git commit -m '添加新功能'`)
4. 推送到分支 (`git push origin feature/your-feature`)
5. 提交Pull Request

## 许可证

本项目采用MIT许可证。

## 技术实现细节

### 双臂机器人支持
本项目专门设计用于支持双臂机器人系统，包括左右两个机械臂和夹爪的数据处理。系统能够处理的数据包括：
- 关节位置、速度和力矩数据
- 夹爪状态和控制命令
- 多相机（前/左/右）图像数据

### 数据同步算法
系统使用最近邻算法进行数据同步，而非线性插值，确保处理后的数据不包含合成数据点，保持数据的真实性。

### Web界面实现
Web界面使用Flask框架开发，提供直观的用户交互体验：
- 支持拖放上传ROS包文件
- 显示ROS包元数据（持续时间、话题列表等）
- 提供数据处理任务管理
- 可视化处理结果

### 数据转换流程
1. **原始提取**：保留原始时间戳信息的CSV和图像文件
2. **组帧同步**：按固定频率对齐所有传感器数据
3. **H5转换**：生成结构化HDF5文件
4. **数据重组**：优化数据结构以适应机器学习应用

## 联系与支持

如有任何问题或建议，请通过以下方式联系：

- **项目主页**：[GitHub仓库地址](#)
- **问题报告**：请使用GitHub Issues功能报告问题
- **技术支持**：有关技术支持的问题，请发送电子邮件至[您的邮箱]

## 致谢

- 感谢ROS社区提供的强大工具和库
- 感谢所有对本项目做出贡献的开发者 