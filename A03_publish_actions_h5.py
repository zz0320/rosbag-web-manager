#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import h5py
import numpy as np
import rospy
import argparse
import os
from hdas_msg.msg import motor_control
from std_msgs.msg import Header, Float32
import time
import sys

def publish_actions_from_h5(h5_file=None, rate=10, debug=False, manual=False):
    """
    读取HDF5文件中的机器人动作数据并发布到ROS话题
    适用于convert_combined_fixed.py生成的数据格式
    
    参数:
        h5_file: HDF5文件路径
        rate: 发布频率 (Hz)
        debug: 是否启用调试输出
        manual: 是否启用手动模式，手动模式下需用户确认每次发布
    """
    # 初始化ROS节点
    rospy.init_node('action_publisher_h5', anonymous=True)
    
    # 检查H5文件路径
    if not h5_file:
        rospy.logerr("错误: 必须指定H5文件路径")
        return
    
    if not os.path.exists(h5_file):
        rospy.logerr(f"H5文件不存在: {h5_file}")
        return
    
    # 存储发布者和消息对象
    publishers = {}
    message_objects = {}
    
    # 尝试读取H5文件
    try:
        rospy.loginfo(f"读取H5文件: {h5_file}")
        h5_data = h5py.File(h5_file, 'r')
        
        # 打印H5文件中的所有数据集，帮助调试
        if debug:
            rospy.loginfo("H5文件中的数据集:")
            def print_group(name, obj):
                if isinstance(obj, h5py.Dataset):
                    rospy.loginfo(f"  - 数据集: {name}, 形状: {obj.shape}, 类型: {obj.dtype}")
            h5_data.visititems(print_group)
        
        # 检查必要的数据集是否存在
        required_datasets = ['action/joint/position', 'action/effector/position']
        missing_datasets = [ds for ds in required_datasets if ds not in h5_data]
        if missing_datasets:
            rospy.logerr(f"H5文件缺少必要的数据集: {', '.join(missing_datasets)}")
            return
        
        # 读取关节动作数据 (12维向量：左臂6 + 右臂6)
        joint_actions = h5_data['action/joint/position'][:]
        
        # 验证关节数据维度
        if joint_actions.shape[1] != 12:
            rospy.logerr(f"关节动作数据维度不正确，期望12，实际为{joint_actions.shape[1]}")
            return
        
        # 读取夹爪动作数据 (2维向量：左夹爪1 + 右夹爪1)
        gripper_actions = h5_data['action/effector/position'][:]
        
        # 验证夹爪数据维度
        if gripper_actions.shape[1] != 2:
            rospy.logerr(f"夹爪动作数据维度不正确，期望2，实际为{gripper_actions.shape[1]}")
            return
        
        # 获取数据点数量
        n_samples = joint_actions.shape[0]
        rospy.loginfo(f"H5文件中找到 {n_samples} 个数据点")
        
        # 检查数据点数量是否匹配
        if joint_actions.shape[0] != gripper_actions.shape[0]:
            rospy.logwarn(f"关节和夹爪动作的数据点数量不匹配: 关节={joint_actions.shape[0]}, 夹爪={gripper_actions.shape[0]}")
            n_samples = min(joint_actions.shape[0], gripper_actions.shape[0])
            rospy.logwarn(f"将使用两者中较小的数量: {n_samples}")
        
        # 分离左右臂数据
        left_arm_actions = joint_actions[:, :6]  # 前6维是左臂
        right_arm_actions = joint_actions[:, 6:12]  # 后6维是右臂
        
        # 分离左右夹爪数据
        left_gripper_actions = gripper_actions[:, 0]  # 第1维是左夹爪
        right_gripper_actions = gripper_actions[:, 1]  # 第2维是右夹爪
        
        # 检查数据是否含有NaN或无穷大值
        has_nan_inf = False
        if np.isnan(joint_actions).any() or np.isinf(joint_actions).any():
            rospy.logwarn("关节动作数据包含NaN或无穷大值")
            has_nan_inf = True
        
        if np.isnan(gripper_actions).any() or np.isinf(gripper_actions).any():
            rospy.logwarn("夹爪动作数据包含NaN或无穷大值")
            has_nan_inf = True
        
        if has_nan_inf:
            if debug:
                rospy.loginfo("数据中的NaN/无穷大值将在发布前被替换为0")
        
        # 初始化左臂发布者和消息对象
        publishers['left_arm'] = rospy.Publisher('/motion_control/control_arm_left', motor_control, queue_size=10)
        left_arm_msg = motor_control()
        left_arm_msg.name = "left_arm"
        left_arm_msg.v_des = [12, 12, 12, 12, 12, 12]
        left_arm_msg.kp = [0]
        left_arm_msg.kd = [0]
        left_arm_msg.t_ff = [0.800000011920929, 0.800000011920929, 0.800000011920929, 0.800000011920929, 0.800000011920929, 0.800000011920929]
        message_objects['left_arm'] = left_arm_msg
        rospy.loginfo(f"左臂数据准备就绪: {left_arm_actions.shape[0]} 个采样点")
        
        # 初始化右臂发布者和消息对象
        publishers['right_arm'] = rospy.Publisher('/motion_control/control_arm_right', motor_control, queue_size=10)
        right_arm_msg = motor_control()
        right_arm_msg.name = "right_arm"
        right_arm_msg.v_des = [12, 12, 12, 12, 12, 12]
        right_arm_msg.kp = [0]
        right_arm_msg.kd = [0]
        right_arm_msg.t_ff = [0.800000011920929, 0.800000011920929, 0.800000011920929, 0.800000011920929, 0.800000011920929, 0.800000011920929]
        message_objects['right_arm'] = right_arm_msg
        rospy.loginfo(f"右臂数据准备就绪: {right_arm_actions.shape[0]} 个采样点")
        
        # 初始化左夹爪发布者和消息对象
        publishers['left_gripper'] = rospy.Publisher('/motion_control/position_control_gripper_left', Float32, queue_size=10)
        message_objects['left_gripper'] = Float32()
        rospy.loginfo(f"左夹爪数据准备就绪: {len(left_gripper_actions)} 个采样点")
        
        # 初始化右夹爪发布者和消息对象
        publishers['right_gripper'] = rospy.Publisher('/motion_control/position_control_gripper_right', Float32, queue_size=10)
        message_objects['right_gripper'] = Float32()
        rospy.loginfo(f"右夹爪数据准备就绪: {len(right_gripper_actions)} 个采样点")
        
        # 创建速率控制器
        rate_controller = rospy.Rate(rate)
        
        if manual:
            rospy.loginfo(f"已启用手动模式，将一个一个发布{n_samples}个动作消息...")
            rospy.loginfo("按 Enter 发布下一个动作，输入 'q' 退出，输入数字跳转到指定索引")
        else:
            rospy.loginfo(f"开始以{rate}Hz的频率发布{n_samples}个同步动作消息...")
        
        rospy.loginfo(f"已启用的发布者: {', '.join(publishers.keys())}")
        
        try:
            i = 0
            while i < n_samples:
                # 检查是否需要退出
                if rospy.is_shutdown():
                    break
                
                # 手动模式下等待用户输入
                if manual:
                    user_input = input(f"按Enter发布下一个动作({i+1}/{n_samples})，输入'q'退出，输入数字跳转: ")
                    if user_input.lower() == 'q':
                        rospy.loginfo("用户请求退出")
                        break
                    elif user_input.isdigit():
                        target_idx = int(user_input)
                        if 1 <= target_idx <= n_samples:
                            i = target_idx - 1
                            rospy.loginfo(f"跳转到动作 {i+1}")
                        else:
                            rospy.logwarn(f"无效索引 {target_idx}，有效范围: 1-{n_samples}")
                            continue
                
                # 使用rospy获取最新的时间戳 - 所有消息使用同一时间戳
                current_time = rospy.Time.now()
                
                # 设置消息头
                header = Header()
                header.stamp = current_time
                
                # 存储本轮要发布的消息值
                message_values = {}
                
                # 准备左臂消息
                message_objects['left_arm'].header = header
                # 替换NaN和无穷大值为0
                left_arm_data = left_arm_actions[i].copy()
                left_arm_data = np.nan_to_num(left_arm_data, nan=0.0, posinf=0.0, neginf=0.0)
                message_objects['left_arm'].p_des = left_arm_data.tolist()
                message_values['left_arm'] = message_objects['left_arm'].p_des
                
                # 准备右臂消息
                message_objects['right_arm'].header = header
                # 替换NaN和无穷大值为0
                right_arm_data = right_arm_actions[i].copy()
                right_arm_data = np.nan_to_num(right_arm_data, nan=0.0, posinf=0.0, neginf=0.0)
                message_objects['right_arm'].p_des = right_arm_data.tolist()
                message_values['right_arm'] = message_objects['right_arm'].p_des
                
                # 准备左夹爪消息
                left_gripper_value = float(left_gripper_actions[i])
                # 替换NaN和无穷大值为0
                if np.isnan(left_gripper_value) or np.isinf(left_gripper_value):
                    left_gripper_value = 0.0
                message_objects['left_gripper'].data = left_gripper_value
                message_values['left_gripper'] = message_objects['left_gripper'].data
                
                # 准备右夹爪消息
                right_gripper_value = float(right_gripper_actions[i])
                # 替换NaN和无穷大值为0
                if np.isnan(right_gripper_value) or np.isinf(right_gripper_value):
                    right_gripper_value = 0.0
                message_objects['right_gripper'].data = right_gripper_value
                message_values['right_gripper'] = message_objects['right_gripper'].data
                
                # 同步发布所有消息
                for key in publishers:
                    publishers[key].publish(message_objects[key])
                
                # 打印当前发布的数据
                if debug or manual or i % 50 == 0:
                    print("=" * 50)
                    print(f"发布数据 {i+1}/{n_samples} (时间戳: {current_time.to_sec()}):")
                    for key in message_values:
                        print(f"{key}: {message_values[key]}")
                    print("=" * 50)
                
                # 增加索引
                i += 1
                
                # 非手动模式下控制发布频率
                if not manual:
                    rate_controller.sleep()
                
        except KeyboardInterrupt:
            rospy.loginfo("用户中断，停止发布...")
        finally:
            rospy.loginfo("完成动作发布")
            h5_data.close()
            
    except Exception as e:
        rospy.logerr(f"处理H5文件时出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='从H5文件读取机器人动作并发布到ROS话题')
    parser.add_argument('--h5_file', type=str, required=True,
                       help='包含机器人动作数据的H5文件路径')
    parser.add_argument('--rate', type=int, default=10, help='发布频率 (Hz)')
    parser.add_argument('--debug', action='store_true', help='启用调试输出')
    parser.add_argument('--manual', action='store_true', help='启用手动模式，一个一个发布动作')
    
    args = parser.parse_args()
    
    # 检查是否指定了H5文件
    if not args.h5_file:
        print("错误: 必须指定H5文件路径")
        parser.print_help()
        sys.exit(1)
    
    publish_actions_from_h5(args.h5_file, args.rate, args.debug, args.manual) 