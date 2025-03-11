#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ROS包数据提取工具

此脚本用于从ROS包文件中提取特定话题的数据，并将其保存为CSV和图像文件。
主要功能包括：
1. 提取机械臂和夹爪的关节状态数据
2. 提取机械臂和夹爪的控制命令数据
3. 提取相机图像数据

使用方法:
    python3 rosbag_extractor.py <bag_file_path> [output_directory]

参数:
    bag_file_path: ROS包文件路径
    output_directory: 可选，输出目录路径。如果不提供，将使用当前目录下以时间戳命名的文件夹

输出:
    - csv/left_arm_states.csv: 左臂关节状态数据
    - csv/right_arm_states.csv: 右臂关节状态数据
    - csv/left_arm_actions.csv: 左臂控制命令数据
    - csv/right_arm_actions.csv: 右臂控制命令数据
    - csv/left_gripper_states.csv: 左夹爪状态数据（包含位置、速度、力矩）
    - csv/right_gripper_states.csv: 右夹爪状态数据（包含位置、速度、力矩）
    - csv/left_gripper_actions.csv: 左夹爪控制命令数据
    - csv/right_gripper_actions.csv: 右夹爪控制命令数据
    - csv/left_image_info.csv: 左相机图像信息数据
    - csv/right_image_info.csv: 右相机图像信息数据
    - csv/left_wrist_image_info.csv: 左腕部相机图像信息数据
    - csv/right_wrist_image_info.csv: 右腕部相机图像信息数据
    - images/left/: 包含左相机提取的图像数据的目录
    - images/right/: 包含右相机提取的图像数据的目录
    - images/left_wrist/: 包含左腕部相机提取的图像数据的目录
    - images/right_wrist/: 包含右腕部相机提取的图像数据的目录

作者: OpenLoong团队
"""

import csv
import datetime
import json
import os
import sys
import traceback

import cv2
import numpy as np
import rosbag
import yaml
from cv_bridge import CvBridge
from sensor_msgs.msg import CompressedImage, Image, JointState

# 常量定义
# =========================================================

# 默认要处理的话题列表
DEFAULT_TOPICS = [
    # 相机话题
    "/hdas/camera_head/left_raw/image_raw_color/compressed",
    "/hdas/camera_head/right_raw/image_raw_color/compressed",
    
    # 新增腕部相机话题
    "/hdas/camera_wrist/left_raw/image_raw_color/compressed",
    "/hdas/camera_wrist/right_raw/image_raw_color/compressed",
    
    # 机械臂关节反馈话题 (JointState)
    "/hdas/feedback_arm_left",
    "/hdas/feedback_arm_right",

    # 夹爪反馈话题 (JointState)
    "/hdas/feedback_gripper_left",
    "/hdas/feedback_gripper_right",

    # 机械臂控制话题 (motor_control)
    "/motion_control/control_arm_left",
    "/motion_control/control_arm_right",
    
    # 夹爪位置控制话题 (Float32)
    "/motion_control/position_control_gripper_left",
    "/motion_control/position_control_gripper_right",
]

# 话题类型映射
TOPIC_TYPE_MAP = {
    # JointState类型话题
    "/hdas/feedback_arm_left": "sensor_msgs/JointState",
    "/hdas/feedback_arm_right": "sensor_msgs/JointState",
    "/hdas/feedback_gripper_left": "sensor_msgs/JointState",
    "/hdas/feedback_gripper_right": "sensor_msgs/JointState",
    
    # 状态反馈话题
    "/hdas/state_gripper_left": "hdas_msg/feedback_status",
    "/hdas/state_gripper_right": "hdas_msg/feedback_status",
    
    # 电机控制话题
    "/motion_control/control_arm_left": "hdas_msg/motor_control",
    "/motion_control/control_arm_right": "hdas_msg/motor_control",
    "/motion_control/control_gripper_left": "hdas_msg/motor_control",
    "/motion_control/control_gripper_right": "hdas_msg/motor_control",
    
    # 夹爪位置控制话题
    "/motion_control/position_control_gripper_left": "std_msgs/Float32",
    "/motion_control/position_control_gripper_right": "std_msgs/Float32",
    
    # 图像话题
    "/hdas/camera_head/left_raw/image_raw_color/compressed": "sensor_msgs/CompressedImage",
    "/hdas/camera_head/right_raw/image_raw_color/compressed": "sensor_msgs/CompressedImage",
    
    # 新增腕部相机话题类型
    "/hdas/camera_wrist/left_raw/image_raw_color/compressed": "sensor_msgs/CompressedImage",
    "/hdas/camera_wrist/right_raw/image_raw_color/compressed": "sensor_msgs/CompressedImage"
}

# 关节名称映射（为输出CSV提供可读性）
ARM_JOINT_NAMES = [
    "joint1", "joint2", "joint3", "joint4", "joint5", "joint6", "gripper_joint"
]

# CSV列名定义
LEFT_ARM_STATES_CSV_HEADER = [
    "timestamp", 
    "left_arm_pos_1", "left_arm_pos_2", "left_arm_pos_3", "left_arm_pos_4", "left_arm_pos_5", "left_arm_pos_6"
]

RIGHT_ARM_STATES_CSV_HEADER = [
    "timestamp", 
    "right_arm_pos_1", "right_arm_pos_2", "right_arm_pos_3", "right_arm_pos_4", "right_arm_pos_5", "right_arm_pos_6"
]

LEFT_ARM_ACTIONS_CSV_HEADER = [
    "timestamp", 
    "left_arm_exp_1", "left_arm_exp_2", "left_arm_exp_3", "left_arm_exp_4", "left_arm_exp_5", "left_arm_exp_6"
]

RIGHT_ARM_ACTIONS_CSV_HEADER = [
    "timestamp", 
    "right_arm_exp_1", "right_arm_exp_2", "right_arm_exp_3", "right_arm_exp_4", "right_arm_exp_5", "right_arm_exp_6"
]

LEFT_GRIPPER_STATES_CSV_HEADER = [
    "timestamp", 
    "left_gripper_pos"
]

RIGHT_GRIPPER_STATES_CSV_HEADER = [
    "timestamp", 
    "right_gripper_pos"
]

LEFT_GRIPPER_ACTIONS_CSV_HEADER = [
    "timestamp", 
    "left_gripper_exp"
]

RIGHT_GRIPPER_ACTIONS_CSV_HEADER = [
    "timestamp", 
    "right_gripper_exp"
]

# 图像信息CSV表头
LEFT_IMAGE_INFO_CSV_HEADER = [
    "timestamp", 
    "image_path", "width", "height"
]

RIGHT_IMAGE_INFO_CSV_HEADER = [
    "timestamp", 
    "image_path", "width", "height"
]

# 新增腕部相机图像信息CSV表头
LEFT_WRIST_IMAGE_INFO_CSV_HEADER = [
    "timestamp", 
    "image_path", "width", "height"
]

RIGHT_WRIST_IMAGE_INFO_CSV_HEADER = [
    "timestamp", 
    "image_path", "width", "height"
]

# 组合数据CSV表头（保留用于内部处理）
COMBINED_DATA_CSV_HEADER = [
    "timestamp",
    # 左臂状态
    "left_arm_pos_1", "left_arm_pos_2", "left_arm_pos_3", "left_arm_pos_4", "left_arm_pos_5", "left_arm_pos_6",
    # 右臂状态
    "right_arm_pos_1", "right_arm_pos_2", "right_arm_pos_3", "right_arm_pos_4", "right_arm_pos_5", "right_arm_pos_6",
    # 夹爪状态
    "left_gripper_pos", "right_gripper_pos",
    # 左臂控制
    "left_arm_exp_1", "left_arm_exp_2", "left_arm_exp_3", "left_arm_exp_4", "left_arm_exp_5", "left_arm_exp_6",
    # 右臂控制
    "right_arm_exp_1", "right_arm_exp_2", "right_arm_exp_3", "right_arm_exp_4", "right_arm_exp_5", "right_arm_exp_6",
    # 夹爪控制
    "left_gripper_exp", "right_gripper_exp"
]

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def extract_bag_info(bag):
    """提取ROS包的基本信息"""
    try:
        # 如果是bag对象，直接使用
        if isinstance(bag, rosbag.bag.Bag):
            start_time = bag.get_start_time()
            end_time = bag.get_end_time()
            
            # 转换为可读的时间格式
            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
            
            return {
                'start_time': start_time_str,
                'end_time': end_time_str,
                'duration': end_time - start_time
            }
        # 如果是路径字符串，打开bag
        elif isinstance(bag, (str, bytes, os.PathLike)):
            info_dict = yaml.load(rosbag.Bag(bag, 'r')._get_yaml_info(), Loader=yaml.SafeLoader)
            start_time = info_dict['start']
            end_time = info_dict['end']
            
            # 转换为可读的时间格式
            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
            
            return {
                'start_time': start_time_str,
                'end_time': end_time_str,
                'duration': end_time - start_time
            }
        else:
            raise TypeError(f"期望接收Bag对象或文件路径，但接收到的是: {type(bag)}")
    except Exception as e:
        print(f"提取包信息时出错: {e}")
        return {
            'start_time': "未知",
            'end_time': "未知",
            'duration': 0
        }

def process_arm_joint_state_msg(msg, csv_writer, timestamp, time_str):
    """处理机械臂JointState消息并写入CSV"""
    try:
        # 机械臂有7个关节（6个臂关节+1个夹爪关节）
        for i in range(min(len(msg.position), 7)):
            joint_name = ARM_JOINT_NAMES[i] if i < len(ARM_JOINT_NAMES) else f"joint{i+1}"
            position = msg.position[i] if i < len(msg.position) else 'None'
            velocity = msg.velocity[i] if i < len(msg.velocity) else 'None'
            effort = msg.effort[i] if i < len(msg.effort) else 'None'
            csv_writer.writerow([timestamp, time_str, joint_name, position, velocity, effort])
    except Exception as e:
        print(f"处理机械臂JointState消息时出错: {e}")
        traceback.print_exc()

def process_gripper_joint_state_msg(msg, csv_writer, timestamp, time_str):
    """处理夹爪JointState消息并写入CSV"""
    try:
        # 夹爪只关心位置值（行程）
        position = msg.position[0] if len(msg.position) > 0 else 'None'
        csv_writer.writerow([timestamp, time_str, position])
    except Exception as e:
        print(f"处理夹爪JointState消息时出错: {e}")
        traceback.print_exc()

def process_feedback_status_msg(msg, csv_writer, timestamp, time_str):
    """处理feedback_status自定义消息并写入CSV"""
    try:
        data = {}
        
        # 提取name_id和errors字段
        if hasattr(msg, 'name_id'):
            data['name_id'] = str(msg.name_id)
        
        if hasattr(msg, 'errors'):
            # 处理errors字段，可能是一个复杂结构
            try:
                if isinstance(msg.errors, list):
                    data['errors'] = [str(err) for err in msg.errors]
                else:
                    data['errors'] = str(msg.errors)
            except:
                data['errors'] = "无法解析错误信息"
        
        # 提取其他可能存在的字段
        for attr_name in dir(msg):
            if not attr_name.startswith('_') and attr_name not in ['header', 'deserialize', 'serialize', 'deserialize_numpy', 'serialize_numpy', 'name_id', 'errors']:
                data[attr_name] = str(getattr(msg, attr_name))
        
        csv_writer.writerow([timestamp, time_str, json.dumps(data, ensure_ascii=False)])
    except Exception as e:
        print(f"处理feedback_status消息时出错: {e}")
        traceback.print_exc()

def safe_convert_to_float_list(value_list, expected_length=None):
    """安全地将值列表转换为浮点数列表
    
    Args:
        value_list: 要转换的值列表
        expected_length: 期望的列表长度，如果提供，将确保返回的列表长度为此值
        
    Returns:
        转换后的浮点数列表
    """
    result = []
    
    # 如果输入为None或空，返回空列表或指定长度的零列表
    if value_list is None:
        return [0.0] * expected_length if expected_length is not None else []
    
    # 尝试转换列表中的每个元素
    try:
        for value in value_list:
            try:
                result.append(float(value))
            except (ValueError, TypeError) as e:
                print(f"无法将 {value} 转换为浮点数: {e}")
                result.append(0.0)
    except TypeError as e:
        # 如果value_list不可迭代，尝试直接转换
        print(f"输入不是列表或可迭代对象: {e}")
        try:
            result = [float(value_list)]
        except (ValueError, TypeError):
            result = [0.0]
    
    # 如果指定了期望长度，确保结果列表长度符合要求
    if expected_length is not None:
        # 如果列表太短，用0.0填充
        while len(result) < expected_length:
            result.append(0.0)
        # 如果列表太长，截断
        if len(result) > expected_length:
            result = result[:expected_length]
    
    return result

def process_arm_motor_controls(bag, topics, data_cache):
    """处理机械臂控制命令数据"""
    print("处理机械臂控制命令数据...")
    # 添加计数器
    message_count = {topic: 0 for topic in topics}
    
    for topic in topics:
        is_left = 'left' in topic
        
        for _, msg, t in bag.read_messages(topics=[topic]):
            try:
                # 使用纳秒时间戳
                timestamp_ns = t.to_nsec()
                
                # 增加计数器
                message_count[topic] += 1
                
                # 输出第一条消息的字段，用于调试
                if message_count[topic] == 1:
                    print(f"\n首条 {topic} 消息字段:")
                    for attr_name in dir(msg):
                        # 跳过内部或特殊属性
                        if attr_name.startswith('_') or attr_name in ['serialize', 'deserialize']:
                            continue
                        try:
                            attr_value = getattr(msg, attr_name)
                            # 检查是否是方法
                            if not callable(attr_value):
                                print(f" - {attr_name}: {attr_value} (类型: {type(attr_value)})")
                        except Exception as e:
                            print(f" - {attr_name}: 无法访问 ({e})")
                
                # 提取关节位置控制数据
                positions = []
                
                # 尝试从不同类型的消息中提取关节位置控制信息
                # 根据publish_actions.py，主要应该检查p_des字段
                if hasattr(msg, 'p_des'):
                    # 这是hdas_msg/motor_control消息的主要字段
                    positions = safe_convert_to_float_list(msg.p_des, 6)
                    if message_count[topic] == 1:
                        print(f"从p_des字段提取位置: {positions}")
                elif hasattr(msg, 'position'):
                    # JointState类型消息
                    positions = safe_convert_to_float_list(msg.position, 6)
                elif hasattr(msg, 'joint_positions'):
                    # 自定义类型消息
                    positions = safe_convert_to_float_list(msg.joint_positions, 6)
                elif hasattr(msg, 'data'):
                    # Float32MultiArray类型消息
                    positions = safe_convert_to_float_list(msg.data, 6)
                
                # 确保有6个关节值
                while len(positions) < 6:
                    positions.append(0.0)
                
                # 根据左右臂填充对应的数据字典
                if is_left:
                    # 为左臂控制数据创建条目
                    if timestamp_ns not in data_cache['left_actions_data']:
                        data_cache['left_actions_data'][timestamp_ns] = [timestamp_ns] + list(positions)
                    else:
                        # 更新现有条目
                        for i, pos in enumerate(positions):
                            data_cache['left_actions_data'][timestamp_ns][i+1] = pos
                else:
                    # 为右臂控制数据创建条目
                    if timestamp_ns not in data_cache['right_actions_data']:
                        data_cache['right_actions_data'][timestamp_ns] = [timestamp_ns] + list(positions)
                    else:
                        # 更新现有条目
                        for i, pos in enumerate(positions):
                            data_cache['right_actions_data'][timestamp_ns][i+1] = pos
            
            except Exception as e:
                print(f"处理机械臂控制命令消息时出错: {e}")
                traceback.print_exc()
    
    # 输出处理的消息数量
    print("机械臂控制命令数据处理完成，各话题处理的消息数量:")
    for topic, count in message_count.items():
        print(f" - {topic}: {count}条消息")
    print(f"左臂控制数据条目数: {len(data_cache['left_actions_data'])}")
    print(f"右臂控制数据条目数: {len(data_cache['right_actions_data'])}")

def check_motor_control_message_format(bag, topics):
    """检查机械臂控制命令消息的格式"""
    print("\n==== 检查机械臂控制命令消息格式 ====")
    
    for topic in topics:
        print(f"检查话题 {topic} 的消息格式...")
        message_found = False
        
        for _, msg, _ in bag.read_messages(topics=[topic], start_time=None, end_time=None):
            message_found = True
            print(f"\n话题 {topic} 的消息类型: {type(msg).__name__}")
            print("消息字段:")
            
            # 获取消息的所有属性
            attrs = dir(msg)
            for attr in attrs:
                # 跳过内部或特殊属性
                if attr.startswith('_') or attr in ['serialize', 'deserialize']:
                    continue
                
                try:
                    attr_value = getattr(msg, attr)
                    # 检查是否是方法
                    if not callable(attr_value):
                        # 对于列表类型，获取其长度和类型
                        if isinstance(attr_value, (list, tuple)):
                            if len(attr_value) > 0:
                                print(f" - {attr}: 列表，长度={len(attr_value)}，类型={type(attr_value[0]).__name__}")
                                # 对于较短的列表，打印其内容
                                if len(attr_value) <= 10:
                                    print(f"   值: {attr_value}")
                            else:
                                print(f" - {attr}: 空列表")
                        else:
                            print(f" - {attr}: {attr_value} (类型: {type(attr_value).__name__})")
                except Exception as e:
                    print(f" - {attr}: 无法访问 ({e})")
            
            # 只检查第一条消息
            break
        
        if not message_found:
            print(f"未找到话题 {topic} 的消息")
    
    print("==== 消息格式检查结束 ====\n")

def safe_convert_to_float(value, default=0.0):
    """安全地将值转换为浮点数
    
    Args:
        value: 要转换的值
        default: 转换失败时返回的默认值
        
    Returns:
        转换后的浮点数或默认值
    """
    if value is None:
        return default
    
    try:
        # 如果是元组或列表，取第一个元素
        if isinstance(value, (list, tuple)) and len(value) > 0:
            return float(value[0])
        
        # 否则直接转换
        return float(value)
    except (ValueError, TypeError) as e:
        print(f"无法将 {value} 转换为浮点数: {e}")
        return default

def process_gripper_motor_controls(bag, topics, data_cache):
    """处理夹爪电机控制命令数据"""
    print("处理夹爪电机控制命令数据...")
    for topic in topics:
        is_left = 'left' in topic
        
        for _, msg, t in bag.read_messages(topics=[topic]):
            try:
                timestamp = t.to_sec()
                
                # 提取期望位置数据
                position = 0.0
                if hasattr(msg, 'data'):
                    position = safe_convert_to_float(msg.data)
                elif hasattr(msg, 'p_des') and len(msg.p_des) > 0:
                    position = safe_convert_to_float(msg.p_des[0])
                
                # 根据左右夹爪填充对应的数据字典
                if is_left:
                    # 为左夹爪控制命令数据创建条目
                    if timestamp not in data_cache['left_gripper_actions_data']:
                        data_cache['left_gripper_actions_data'][timestamp] = [timestamp, position]
                    else:
                        data_cache['left_gripper_actions_data'][timestamp][1] = position
                else:
                    # 为右夹爪控制命令数据创建条目
                    if timestamp not in data_cache['right_gripper_actions_data']:
                        data_cache['right_gripper_actions_data'][timestamp] = [timestamp, position]
                    else:
                        data_cache['right_gripper_actions_data'][timestamp][1] = position
            
            except Exception as e:
                print(f"处理夹爪电机控制命令消息时出错: {e}")
                traceback.print_exc()

def process_float32_msg(msg, csv_writer, timestamp, time_str):
    """处理Float32消息并写入CSV"""
    try:
        value = msg.data if hasattr(msg, 'data') else 'None'
        csv_writer.writerow([timestamp, time_str, value])
    except Exception as e:
        print(f"处理Float32消息时出错: {e}")
        traceback.print_exc()

def process_bag(bag_path, output_dir=None, topics=None):
    """处理ROS包并提取传感器数据
    
    Args:
        bag_path: ROS包文件路径
        output_dir: 输出目录，默认为ROS包所在目录
        topics: 要处理的话题列表，默认为DEFAULT_TOPICS
        
    Returns:
        成功时返回True，失败时返回False
        
    处理的话题包括:
        - 机械臂关节状态: /hdas/feedback_arm_left, /hdas/feedback_arm_right
        - 夹爪关节状态: /hdas/feedback_gripper_left, /hdas/feedback_gripper_right
        - 机械臂控制命令: /motion_control/control_arm_left, /motion_control/control_arm_right
        - 夹爪控制命令: /motion_control/position_control_gripper_left, /motion_control/position_control_gripper_right
        - 头部相机图像: /hdas/camera_head/left_raw/image_raw_color/compressed, /hdas/camera_head/right_raw/image_raw_color/compressed
        - 腕部相机图像: /hdas/camera_wrist/left_raw/image_raw_color/compressed, /hdas/camera_wrist/right_raw/image_raw_color/compressed
    """
    try:
        # 设置默认值
        if topics is None:
            topics = DEFAULT_TOPICS
        
        if output_dir is None:
            # 默认输出到ROS包所在目录下的同名文件夹
            bag_name = os.path.splitext(os.path.basename(bag_path))[0]
            output_dir = os.path.join(os.path.dirname(bag_path), bag_name)
        
        # 创建输出目录
        ensure_dir(output_dir)
        
        # 创建CSV和图像目录
        csv_dir = os.path.join(output_dir, 'csv')
        images_dir = os.path.join(output_dir, 'images')
        ensure_dir(csv_dir)
        ensure_dir(images_dir)
        
        # 打开ROS包
        print(f"打开ROS包: {bag_path}")
        bag = rosbag.Bag(bag_path)
        
        # 打印包中包含的所有话题
        print_bag_topics(bag)
        
        # 提取包信息
        bag_info = extract_bag_info(bag)
        print(f"包开始时间: {bag_info['start_time']}")
        print(f"包结束时间: {bag_info['end_time']}")
        print(f"包持续时间: {bag_info['duration']:.2f}秒")
        
        # 获取包中的所有话题
        bag_topics = bag.get_type_and_topic_info()[1]
        
        # 过滤出存在于包中的话题
        filtered_topics = []
        for topic in topics:
            if topic in bag_topics:
                filtered_topics.append(topic)
                print(f"找到话题: {topic}")
            else:
                print(f"警告: 话题 {topic} 在包中不存在")
        
        # 创建CV桥接
        bridge = CvBridge()
        
        # 将话题分类
        topic_categories = classify_topics(filtered_topics, TOPIC_TYPE_MAP)
        
        # 特别检查机械臂控制命令话题
        if topic_categories['arm_control_topics']:
            check_motor_control_message_format(bag, topic_categories['arm_control_topics'])
        
        # 创建CSV文件
        csv_files = create_csv_files(csv_dir)
        
        # 处理压缩图像
        image_info = process_compressed_images(bag, topic_categories['compressed_image_topics'], bridge, images_dir)
        
        # 处理传感器数据
        data_cache = process_bag_data(bag, topic_categories)
        
        # 将数据写入CSV文件
        write_data_to_csv(data_cache, csv_files)
        
        # 将图像信息写入CSV
        write_image_info_to_csv(image_info, csv_files)
        
        # 关闭包
        bag.close()
        
        # 打印处理统计
        print_statistics(topic_categories, csv_files, output_dir)
        print("数据已以纳秒精度处理和保存")
        
        return True
    
    except Exception as e:
        print(f"处理ROS包时出错: {e}")
        traceback.print_exc()
        return False

def classify_topics(topics, topic_type_map):
    """将话题分类为不同类别"""
    topic_categories = {
        'joint_state_topics': [],
        'arm_joint_state_topics': [],
        'gripper_state_topics': [],
        'arm_control_topics': [],
        'gripper_position_control_topics': [],
        'compressed_image_topics': [],
        'all_topics': []
    }
    
    for topic in topics:
        # 添加到所有话题列表
        topic_categories['all_topics'].append(topic)
        
        # 根据话题类型和名称进行分类
        topic_type = topic_type_map.get(topic, "")
        
        # 处理JointState类型话题
        if topic_type == "sensor_msgs/JointState":
            topic_categories['joint_state_topics'].append(topic)
            
            # 进一步区分机械臂和夹爪的JointState话题
            if 'arm' in topic:
                topic_categories['arm_joint_state_topics'].append(topic)
            elif 'gripper' in topic:
                topic_categories['gripper_state_topics'].append(topic)
        
        # 处理机械臂控制话题
        elif topic_type == "hdas_msg/motor_control" and 'arm' in topic:
            topic_categories['arm_control_topics'].append(topic)
        
        # 处理夹爪位置控制话题
        elif topic_type == "std_msgs/Float32" and 'gripper' in topic:
            topic_categories['gripper_position_control_topics'].append(topic)
        
        # 处理压缩图像话题
        elif topic_type == "sensor_msgs/CompressedImage":
            topic_categories['compressed_image_topics'].append(topic)
    
    return topic_categories

def print_bag_topics(bag):
    """打印ROS包中包含的所有话题"""
    print("\n==== 检查ROS包中的话题 ====")
    topics_info = bag.get_type_and_topic_info()
    topics = topics_info.topics
    
    print(f"ROS包包含 {len(topics)} 个话题:")
    for topic_name, topic_info in topics.items():
        msg_count = topic_info.message_count
        msg_type = topic_info.msg_type
        print(f" - {topic_name} [{msg_type}]: {msg_count}条消息")
    
    print("==== 话题检查结束 ====\n")

def create_csv_files(csv_dir):
    """创建并初始化CSV文件"""
    # 机械臂CSV文件路径
    left_arm_states_csv_path = os.path.join(csv_dir, 'left_arm_states.csv')
    right_arm_states_csv_path = os.path.join(csv_dir, 'right_arm_states.csv')
    left_arm_actions_csv_path = os.path.join(csv_dir, 'left_arm_actions.csv')
    right_arm_actions_csv_path = os.path.join(csv_dir, 'right_arm_actions.csv')
    
    # 夹爪CSV文件路径
    left_gripper_states_csv_path = os.path.join(csv_dir, 'left_gripper_states.csv')
    right_gripper_states_csv_path = os.path.join(csv_dir, 'right_gripper_states.csv')
    left_gripper_actions_csv_path = os.path.join(csv_dir, 'left_gripper_actions.csv')
    right_gripper_actions_csv_path = os.path.join(csv_dir, 'right_gripper_actions.csv')
    
    # 图像信息CSV文件路径
    left_image_info_csv_path = os.path.join(csv_dir, 'left_image_info.csv')
    right_image_info_csv_path = os.path.join(csv_dir, 'right_image_info.csv')
    
    # 腕部相机图像信息CSV文件路径
    left_wrist_image_info_csv_path = os.path.join(csv_dir, 'left_wrist_image_info.csv')
    right_wrist_image_info_csv_path = os.path.join(csv_dir, 'right_wrist_image_info.csv')
    
    # 创建并写入表头 - 机械臂
    with open(left_arm_states_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(LEFT_ARM_STATES_CSV_HEADER)
    
    with open(right_arm_states_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(RIGHT_ARM_STATES_CSV_HEADER)
    
    with open(left_arm_actions_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(LEFT_ARM_ACTIONS_CSV_HEADER)
    
    with open(right_arm_actions_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(RIGHT_ARM_ACTIONS_CSV_HEADER)
    
    # 创建并写入表头 - 夹爪
    with open(left_gripper_states_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(LEFT_GRIPPER_STATES_CSV_HEADER)
    
    with open(right_gripper_states_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(RIGHT_GRIPPER_STATES_CSV_HEADER)
    
    with open(left_gripper_actions_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(LEFT_GRIPPER_ACTIONS_CSV_HEADER)
    
    with open(right_gripper_actions_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(RIGHT_GRIPPER_ACTIONS_CSV_HEADER)
    
    # 创建并写入表头 - 图像信息
    with open(left_image_info_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(LEFT_IMAGE_INFO_CSV_HEADER)
    
    with open(right_image_info_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(RIGHT_IMAGE_INFO_CSV_HEADER)
    
    # 创建并写入表头 - 腕部相机图像信息
    with open(left_wrist_image_info_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(LEFT_WRIST_IMAGE_INFO_CSV_HEADER)
    
    with open(right_wrist_image_info_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(RIGHT_WRIST_IMAGE_INFO_CSV_HEADER)
    
    return {
        'left_arm_states_csv_path': left_arm_states_csv_path,
        'right_arm_states_csv_path': right_arm_states_csv_path,
        'left_arm_actions_csv_path': left_arm_actions_csv_path,
        'right_arm_actions_csv_path': right_arm_actions_csv_path,
        'left_gripper_states_csv_path': left_gripper_states_csv_path,
        'right_gripper_states_csv_path': right_gripper_states_csv_path,
        'left_gripper_actions_csv_path': left_gripper_actions_csv_path,
        'right_gripper_actions_csv_path': right_gripper_actions_csv_path,
        'left_image_info_csv_path': left_image_info_csv_path,
        'right_image_info_csv_path': right_image_info_csv_path,
        'left_wrist_image_info_csv_path': left_wrist_image_info_csv_path,
        'right_wrist_image_info_csv_path': right_wrist_image_info_csv_path
    }

def process_bag_data(bag, topic_categories):
    """处理包数据并提取到CSV和图像文件"""
    # 创建数据缓存
    data_cache = {
        'left_states_data': {},
        'right_states_data': {},
        'left_actions_data': {},
        'right_actions_data': {},
        'gripper_actions_data': {},
        'left_gripper_states_data': {},
        'right_gripper_states_data': {},
        'left_gripper_actions_data': {},
        'right_gripper_actions_data': {},
        'left_image_info_data': {},
        'right_image_info_data': {},
        'left_wrist_image_info_data': {},  # 新增左腕部相机图像信息数据
        'right_wrist_image_info_data': {},  # 新增右腕部相机图像信息数据
        'combined_data': {}
    }
    
    # 处理机械臂JointState消息
    process_arm_joint_states(bag, topic_categories['arm_joint_state_topics'], data_cache)
    
    # 处理夹爪JointState消息
    process_gripper_joint_states(bag, topic_categories['gripper_state_topics'], data_cache)
    
    # 处理机械臂motor_control消息
    process_arm_motor_controls(bag, topic_categories['arm_control_topics'], data_cache)
    
    # 处理夹爪motor_control消息
    process_gripper_motor_controls(bag, topic_categories['gripper_position_control_topics'], data_cache)
    
    # 生成组合数据
    generate_combined_data(data_cache)
    
    return data_cache

def process_arm_joint_states(bag, topics, data_cache):
    """处理机械臂关节状态数据"""
    print("处理机械臂关节状态数据...")
    for topic in topics:
        is_left = 'left' in topic
        
        for _, msg, t in bag.read_messages(topics=[topic]):
            try:
                # 使用纳秒时间戳而不是秒
                timestamp_ns = t.to_nsec()  # 直接获取纳秒时间戳
                
                # 提取关节位置数据
                positions = safe_convert_to_float_list(msg.position, 7)
                
                # 确保有7个关节值
                while len(positions) < 7:
                    positions.append(0.0)
                
                # 根据左右臂填充对应的数据字典
                if is_left:
                    # 为左臂状态数据创建条目
                    if timestamp_ns not in data_cache['left_states_data']:
                        data_cache['left_states_data'][timestamp_ns] = [timestamp_ns] + [0.0] * (len(LEFT_ARM_STATES_CSV_HEADER) - 1)
                    
                    # 填充左臂状态数据（只处理前6个机械臂关节）
                    for i, pos in enumerate(positions[:6]):
                        data_cache['left_states_data'][timestamp_ns][i+1] = pos
                else:
                    # 为右臂状态数据创建条目
                    if timestamp_ns not in data_cache['right_states_data']:
                        data_cache['right_states_data'][timestamp_ns] = [timestamp_ns] + [0.0] * (len(RIGHT_ARM_STATES_CSV_HEADER) - 1)
                    
                    # 填充右臂状态数据（只处理前6个机械臂关节）
                    for i, pos in enumerate(positions[:6]):
                        data_cache['right_states_data'][timestamp_ns][i+1] = pos
            
            except Exception as e:
                print(f"处理关节状态消息时出错: {e}")
                traceback.print_exc()

def process_gripper_joint_states(bag, topics, data_cache):
    """处理夹爪关节状态数据"""
    print("处理夹爪关节状态数据...")
    for topic in topics:
        is_left = 'left' in topic
        
        for _, msg, t in bag.read_messages(topics=[topic]):
            try:
                # 使用纳秒时间戳而不是秒
                timestamp_ns = t.to_nsec()
                
                # 提取夹爪位置
                position = msg.position[0] if len(msg.position) > 0 else 0.0
                
                # 根据左右夹爪填充对应的数据字典
                if is_left:
                    # 为左夹爪状态数据创建条目
                    data_cache['left_gripper_states_data'][timestamp_ns] = [timestamp_ns, position]
                else:
                    # 为右夹爪状态数据创建条目
                    data_cache['right_gripper_states_data'][timestamp_ns] = [timestamp_ns, position]
            
            except Exception as e:
                print(f"处理夹爪关节状态消息时出错: {e}")
                traceback.print_exc()

def process_compressed_images(bag, topics, bridge, images_dir):
    """处理压缩图像数据并保存为图像文件"""
    print("处理图像数据...")
    
    # 创建相机图像目录
    left_images_dir = os.path.join(images_dir, 'left')
    right_images_dir = os.path.join(images_dir, 'right')
    left_wrist_images_dir = os.path.join(images_dir, 'left_wrist')  # 新增左腕部相机目录
    right_wrist_images_dir = os.path.join(images_dir, 'right_wrist')  # 新增右腕部相机目录
    
    ensure_dir(left_images_dir)
    ensure_dir(right_images_dir)
    ensure_dir(left_wrist_images_dir)  # 确保左腕部相机目录存在
    ensure_dir(right_wrist_images_dir)  # 确保右腕部相机目录存在
    
    # 处理每个图像话题
    image_info = {
        'left': {}, 
        'right': {},
        'left_wrist': {},  # 新增左腕部相机信息
        'right_wrist': {}  # 新增右腕部相机信息
    }
    
    for topic in topics:
        # 判断相机类型
        is_head_camera = 'camera_head' in topic
        is_wrist_camera = 'camera_wrist' in topic
        is_left = 'left' in topic
        
        # 确定输出目录和图像信息类别
        if is_head_camera:
            if is_left:
                output_dir = left_images_dir
                camera_type = 'left'
            else:
                output_dir = right_images_dir
                camera_type = 'right'
        elif is_wrist_camera:
            if is_left:
                output_dir = left_wrist_images_dir
                camera_type = 'left_wrist'
            else:
                output_dir = right_wrist_images_dir
                camera_type = 'right_wrist'
        else:
            # 跳过不符合条件的话题
            continue
        
        for _, msg, t in bag.read_messages(topics=[topic]):
            try:
                # 获取纳秒时间戳
                timestamp_ns = t.to_nsec()
                
                # 将压缩图像转换为OpenCV格式
                np_arr = np.frombuffer(msg.data, np.uint8)
                cv_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                
                # 保存图像 - 使用纳秒时间戳作为文件名
                image_filename = f"{timestamp_ns}.png"
                image_path = os.path.join(output_dir, image_filename)
                cv2.imwrite(image_path, cv_img)
                
                # 保存图像信息
                height, width = cv_img.shape[:2]
                image_info[camera_type][timestamp_ns] = [timestamp_ns, image_path, width, height]
                
            except Exception as e:
                print(f"处理图像时出错: {e}")
                traceback.print_exc()
    
    return image_info

def write_data_to_csv(data_cache, csv_files):
    """将处理后的数据写入CSV文件"""
    # 写入左臂状态数据
    with open(csv_files['left_arm_states_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['left_states_data'].keys()):
            writer.writerow(data_cache['left_states_data'][timestamp])
    
    # 写入右臂状态数据
    with open(csv_files['right_arm_states_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['right_states_data'].keys()):
            writer.writerow(data_cache['right_states_data'][timestamp])
    
    # 写入左臂控制命令数据
    with open(csv_files['left_arm_actions_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['left_actions_data'].keys()):
            writer.writerow(data_cache['left_actions_data'][timestamp])
    
    # 写入右臂控制命令数据
    with open(csv_files['right_arm_actions_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['right_actions_data'].keys()):
            writer.writerow(data_cache['right_actions_data'][timestamp])
    
    # 写入左夹爪状态数据
    with open(csv_files['left_gripper_states_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['left_gripper_states_data'].keys()):
            writer.writerow(data_cache['left_gripper_states_data'][timestamp])
    
    # 写入右夹爪状态数据
    with open(csv_files['right_gripper_states_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['right_gripper_states_data'].keys()):
            writer.writerow(data_cache['right_gripper_states_data'][timestamp])
    
    # 写入左夹爪控制命令数据
    with open(csv_files['left_gripper_actions_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['left_gripper_actions_data'].keys()):
            writer.writerow(data_cache['left_gripper_actions_data'][timestamp])
    
    # 写入右夹爪控制命令数据
    with open(csv_files['right_gripper_actions_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['right_gripper_actions_data'].keys()):
            writer.writerow(data_cache['right_gripper_actions_data'][timestamp])
    
    # 写入左相机图像信息数据
    with open(csv_files['left_image_info_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['left_image_info_data'].keys()):
            writer.writerow(data_cache['left_image_info_data'][timestamp])
    
    # 写入右相机图像信息数据
    with open(csv_files['right_image_info_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(data_cache['right_image_info_data'].keys()):
            writer.writerow(data_cache['right_image_info_data'][timestamp])

def print_statistics(topic_categories, csv_files, output_dir):
    """打印处理统计信息"""
    print("\n处理完成！统计信息:")
    print(f"机械臂关节状态话题数: {len(topic_categories['arm_joint_state_topics'])}")
    print(f"夹爪关节状态话题数: {len(topic_categories['gripper_state_topics'])}")
    print(f"机械臂控制话题数: {len(topic_categories['arm_control_topics'])}")
    print(f"夹爪位置控制话题数: {len(topic_categories['gripper_position_control_topics'])}")
    print(f"压缩图像话题数: {len(topic_categories['compressed_image_topics'])}")
    print(f"所有数据都已保存到目录: {output_dir}")
    print(f"左臂状态数据保存在: {csv_files['left_arm_states_csv_path']}")
    print(f"右臂状态数据保存在: {csv_files['right_arm_states_csv_path']}")
    print(f"左臂控制命令数据保存在: {csv_files['left_arm_actions_csv_path']}")
    print(f"右臂控制命令数据保存在: {csv_files['right_arm_actions_csv_path']}")
    print(f"左夹爪状态数据保存在: {csv_files['left_gripper_states_csv_path']}")
    print(f"右夹爪状态数据保存在: {csv_files['right_gripper_states_csv_path']}")
    print(f"左夹爪控制命令数据保存在: {csv_files['left_gripper_actions_csv_path']}")
    print(f"右夹爪控制命令数据保存在: {csv_files['right_gripper_actions_csv_path']}")
    print(f"左相机图像信息保存在: {csv_files['left_image_info_csv_path']}")
    print(f"右相机图像信息保存在: {csv_files['right_image_info_csv_path']}")
    print(f"左腕部相机图像信息保存在: {csv_files['left_wrist_image_info_csv_path']}")
    print(f"右腕部相机图像信息保存在: {csv_files['right_wrist_image_info_csv_path']}")
    print("\n注意: 现在states.csv中的夹爪位置数据来自/hdas/feedback_gripper_left和/hdas/feedback_gripper_right")
    print("actions.csv中的夹爪控制数据来自/motion_control/position_control_gripper_left和/motion_control/position_control_gripper_right (std_msgs/Float32类型)")

def generate_combined_data(data_cache):
    """生成组合数据，将所有数据整合到一个时间序列中"""
    print("生成纳秒精度的组合数据...")
    
    # 收集所有时间戳
    all_timestamps = set()
    all_timestamps.update(data_cache['left_states_data'].keys())
    all_timestamps.update(data_cache['right_states_data'].keys())
    all_timestamps.update(data_cache['left_actions_data'].keys())
    all_timestamps.update(data_cache['right_actions_data'].keys())
    all_timestamps.update(data_cache['left_gripper_actions_data'].keys())
    all_timestamps.update(data_cache['right_gripper_actions_data'].keys())
    
    print(f"找到 {len(all_timestamps)} 个纳秒级时间戳")
    
    # 对每个时间戳创建组合数据条目
    for timestamp in all_timestamps:
        # 创建默认数据行（全部填充0.0）
        combined_row = [timestamp] + [0.0] * (len(COMBINED_DATA_CSV_HEADER) - 1)
        
        # 填充左臂状态数据（索引1-6）
        if timestamp in data_cache['left_states_data']:
            for i in range(1, min(7, len(data_cache['left_states_data'][timestamp]))):
                combined_row[i] = data_cache['left_states_data'][timestamp][i]
        
        # 填充右臂状态数据（索引7-12）
        if timestamp in data_cache['right_states_data']:
            for i in range(1, min(7, len(data_cache['right_states_data'][timestamp]))):
                combined_row[i+6] = data_cache['right_states_data'][timestamp][i]
        
        # 填充夹爪状态数据（索引13-14）
        if timestamp in data_cache['left_gripper_states_data'] and len(data_cache['left_gripper_states_data'][timestamp]) > 1:
            combined_row[13] = data_cache['left_gripper_states_data'][timestamp][1]  # 左夹爪位置
        if timestamp in data_cache['right_gripper_states_data'] and len(data_cache['right_gripper_states_data'][timestamp]) > 1:
            combined_row[14] = data_cache['right_gripper_states_data'][timestamp][1]  # 右夹爪位置
        
        # 填充左臂控制数据（索引15-20）
        if timestamp in data_cache['left_actions_data']:
            for i in range(1, min(7, len(data_cache['left_actions_data'][timestamp]))):
                combined_row[i+14] = data_cache['left_actions_data'][timestamp][i]
        
        # 填充右臂控制数据（索引21-26）
        if timestamp in data_cache['right_actions_data']:
            for i in range(1, min(7, len(data_cache['right_actions_data'][timestamp]))):
                combined_row[i+20] = data_cache['right_actions_data'][timestamp][i]
        
        # 填充夹爪控制数据（索引27-28）
        if timestamp in data_cache['left_gripper_actions_data'] and len(data_cache['left_gripper_actions_data'][timestamp]) > 1:
            combined_row[27] = data_cache['left_gripper_actions_data'][timestamp][1]  # 左夹爪控制
        if timestamp in data_cache['right_gripper_actions_data'] and len(data_cache['right_gripper_actions_data'][timestamp]) > 1:
            combined_row[28] = data_cache['right_gripper_actions_data'][timestamp][1]  # 右夹爪控制
        
        # 保存组合数据
        data_cache['combined_data'][timestamp] = combined_row
    
    print(f"生成了 {len(data_cache['combined_data'])} 条纳秒精度的组合数据记录")

def write_image_info_to_csv(image_info, csv_files):
    """将图像信息写入CSV文件"""
    # 写入左相机图像信息数据
    with open(csv_files['left_image_info_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(image_info['left'].keys()):
            writer.writerow(image_info['left'][timestamp])
    
    # 写入右相机图像信息数据
    with open(csv_files['right_image_info_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(image_info['right'].keys()):
            writer.writerow(image_info['right'][timestamp])
    
    # 写入左腕部相机图像信息数据
    with open(csv_files['left_wrist_image_info_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(image_info['left_wrist'].keys()):
            writer.writerow(image_info['left_wrist'][timestamp])
    
    # 写入右腕部相机图像信息数据
    with open(csv_files['right_wrist_image_info_csv_path'], 'a', newline='') as f:
        writer = csv.writer(f)
        for timestamp in sorted(image_info['right_wrist'].keys()):
            writer.writerow(image_info['right_wrist'][timestamp])

def main():
    try:
        if len(sys.argv) < 2:
            print("使用方法: python3 rosbag_extractor.py <bag_file_path> [output_directory]")
            sys.exit(1)
        
        bag_path = sys.argv[1]
        
        # 如果未指定输出目录，则使用当前目录下以包名命名的文件夹
        if len(sys.argv) > 2:
            output_dir = sys.argv[2]
        else:
            bag_name = os.path.splitext(os.path.basename(bag_path))[0]
            current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join(os.getcwd(), f"{current_time}")
        
        ensure_dir(output_dir)
        
        if not os.path.exists(bag_path):
            print(f"错误: 找不到包文件 {bag_path}")
            sys.exit(1)
        
        # 使用默认话题列表进行处理
        process_bag(bag_path, output_dir)
    except Exception as e:
        print(f"程序执行过程中发生错误: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 