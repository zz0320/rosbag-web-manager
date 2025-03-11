"""
双臂机器人数据处理工具 - 修改版本

该版本移除了对原始数据的修改操作，仅保留：
1. 组帧同步 (step1_synchronize) - 使用最近邻方法而非插值
2. H5格式生成 (step2_convert_to_h5)
3. 数据格式重构 (step3_reorganize)

移除了的数据处理操作包括：
1. 线性插值（改为使用最近邻方法，保证不生成新数据）
2. 数组维度调整和裁剪 (adjust_array_dimensions, adjust_gripper_array)
3. 数据平滑 (smooth_time_series)
4. 夹爪数据跳变检测和处理 (check_gripper_jumps)
5. 数据范围裁剪 (np.clip)

当遇到NaN和Inf值时，只会报告它们的存在而不替换为0。
"""

#!/usr/bin/env python3
import os
import glob
import shutil
import numpy as np
import pandas as pd
import pickle
import h5py
from tqdm import tqdm
import argparse
from datetime import datetime
from typing import List, Tuple, Dict, Any
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

class CombinedDualArmDataProcessor:
    def __init__(self, data_dir: str, output_base: str, output_dirname: str = None):
        self.data_dir = data_dir
        # H5文件名固定为aligned_joints.h5
        self.output_filename = "aligned_joints.h5"
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_name = os.path.basename(data_dir)
        
        # 创建输出目录结构
        self.sync_base = os.path.join(output_base, "temp_sync", f"{timestamp}_{dir_name}")
        self.sync_dir = os.path.join(self.sync_base, 'final_synchronized')
        
        # 如果指定了输出目录名，则使用它
        if output_dirname:
            self.final_output_dir = os.path.join(output_base, output_dirname)
        else:
            # 默认使用"data_combined"，如果已存在则添加递增序号
            base_dirname = "data_combined"
            suffix = ""
            counter = 1
            
            # 检查目录是否存在，存在则添加递增序号
            while True:
                test_dirname = f"{base_dirname}{suffix}"
                test_path = os.path.join(output_base, test_dirname)
                if not os.path.exists(test_path):
                    break
                suffix = f" ({counter})"
                counter += 1
            
            self.final_output_dir = os.path.join(output_base, test_dirname)
        
        print(f"Processing data from: {self.data_dir}")
        print(f"Output directory: {self.final_output_dir}")
        print(f"Output H5 filename: {self.output_filename} (fixed)")
        
        # 创建必要的目录
        os.makedirs(self.sync_dir, exist_ok=True)
        for cam in ['left', 'right']:  # 只有两个相机
            os.makedirs(os.path.join(self.sync_dir, f'camera_{cam}'), exist_ok=True)
        
        # 创建与原始数据相同的输出目录结构
        os.makedirs(os.path.join(self.final_output_dir, 'camera'), exist_ok=True)
        os.makedirs(os.path.join(self.final_output_dir, 'record'), exist_ok=True)

    def _get_camera_files(self, camera_name: str) -> List[Tuple[str, float]]:
        """查找相机图像文件，支持多种目录结构和文件格式。"""
        # 打印正在搜索的相机
        print(f"Searching for {camera_name} camera images...")
        
        # 尝试多种可能的图像目录路径
        possible_paths = [
            os.path.join(self.data_dir, 'images', f'camera_{camera_name}'),
            os.path.join(self.data_dir, 'images', camera_name),
            os.path.join(self.data_dir, f'camera_{camera_name}'),
            os.path.join(self.data_dir, camera_name),
            # 添加更多可能的路径模式
        ]
        
        # 尝试每个可能的路径，找到第一个存在的
        image_dir = None
        for path in possible_paths:
            if os.path.exists(path) and os.path.isdir(path):
                image_dir = path
                print(f"Found camera directory: {path}")
                break
        
        if image_dir is None:
            # 如果没有找到预定义的路径，尝试在顶级目录中搜索
            for root, dirs, _ in os.walk(self.data_dir):
                for d in dirs:
                    if camera_name in d.lower() and 'camera' in d.lower():
                        image_dir = os.path.join(root, d)
                        print(f"Found camera directory by search: {image_dir}")
                        break
                if image_dir:
                    break
                    
        if image_dir is None:
            raise FileNotFoundError(f"No valid directory found for {camera_name} camera")
                
        # 查找图像文件
        image_files = []
        for ext in ['jpg', 'jpeg', 'png']:
            image_files.extend(glob.glob(os.path.join(image_dir, f"*.{ext}")))
        
        if not image_files:
            raise FileNotFoundError(f"No image files found in {image_dir}")
            
        # 从文件名中提取时间戳
        result = []
        for file in image_files:
            try:
                # 尝试从文件名中提取时间戳
                basename = os.path.basename(file)
                timestamp_str = basename.split('.')[0]  # 移除扩展名
                timestamp = float(timestamp_str)
                result.append((file, timestamp))
            except ValueError:
                # 如果无法将文件名解析为时间戳，则跳过
                continue
                
        if not result:
            raise ValueError(f"Could not extract timestamps from any files in {image_dir}")
            
        # 排序并返回
        return sorted(result, key=lambda x: x[1])

    def _find_closest_file(self, files: List[Tuple[str, float]], target_time: float) -> Tuple[str, float]:
        """找到时间戳最接近目标时间的文件。"""
        # 如果文件列表为空，抛出异常
        if not files:
            raise ValueError("Empty file list provided")
            
        # 如果只有一个文件，直接返回该文件
        if len(files) == 1:
            return files[0]
            
        # 保存原始文件列表
        sorted_files = sorted(files, key=lambda x: x[1])
        
        # 二分查找最接近的时间戳
        left, right = 0, len(sorted_files) - 1
        
        # 如果目标时间小于最小时间戳或大于最大时间戳，直接返回边界值
        if target_time <= sorted_files[left][1]:
            return sorted_files[left]
        if target_time >= sorted_files[right][1]:
            return sorted_files[right]
            
        # 二分查找
        while left <= right:
            mid = (left + right) // 2
            if sorted_files[mid][1] == target_time:
                return sorted_files[mid]
            elif sorted_files[mid][1] < target_time:
                left = mid + 1
            else:
                right = mid - 1
                
        # 此时 left > right，比较 files[right] 和 files[left] 哪个更接近 target_time
        if left >= len(sorted_files):
            closest = sorted_files[right]
        elif right < 0:
            closest = sorted_files[left]
        else:
            if abs(sorted_files[right][1] - target_time) <= abs(sorted_files[left][1] - target_time):
                closest = sorted_files[right]
            else:
                closest = sorted_files[left]
                
        return closest

    def step1_synchronize(self) -> bool:
        """同步所有CSV数据，建立帧索引与时间戳之间的对应关系"""
        try:
            print(f"\n步骤1: 同步数据源 {self.data_dir}")
            
            # 检查输入数据是否存在
            for data_type in ['left_arm', 'right_arm', 'left_gripper', 'right_gripper']:
                state_file = os.path.join(self.data_dir, f'{data_type}_state.csv')
                action_file = os.path.join(self.data_dir, f'{data_type}_action.csv')
                
                if not os.path.exists(state_file):
                    print(f"警告: 状态文件 {state_file} 不存在，将在同步过程中跳过")
                
                if not os.path.exists(action_file):
                    print(f"警告: 动作文件 {action_file} 不存在，将在同步过程中跳过")
            
            # 创建同步目录
            os.makedirs(self.sync_dir, exist_ok=True)
            
            # 尝试读取图像时间戳作为参考
            ref_times = []
            
            # 检查CSV文件目录
            csv_base_dir = os.path.join(self.data_dir, 'csv')
            if not os.path.exists(csv_base_dir):
                print(f"CSV directory not found at {csv_base_dir}")
                # 尝试在顶层目录查找CSV文件
                csv_base_dir = self.data_dir
                csv_files = glob.glob(os.path.join(csv_base_dir, "*.csv"))
                if csv_files:
                    print(f"Found {len(csv_files)} CSV files in main directory")
                else:
                    print(f"No CSV files found in {csv_base_dir}")
                    raise FileNotFoundError("No CSV files found")
            
            # 读取CSV文件
            print("\nReading CSV data files...")
            
            # 定义要处理的CSV文件和备选路径
            csv_filenames = {
                'left_arm_state': ['left_arm_states.csv', 'left_arm_state.csv'],
                'right_arm_state': ['right_arm_states.csv', 'right_arm_state.csv'],
                'left_arm_action': ['left_arm_actions.csv', 'left_arm_action.csv'],
                'right_arm_action': ['right_arm_actions.csv', 'right_arm_action.csv'],
                'left_gripper_state': ['left_gripper_states.csv', 'left_gripper_state.csv'],
                'right_gripper_state': ['right_gripper_states.csv', 'right_gripper_state.csv'],
                'left_gripper_action': ['left_gripper_actions.csv', 'left_gripper_action.csv'],
                'right_gripper_action': ['right_gripper_actions.csv', 'right_gripper_action.csv']
            }
            
            dataframes = {}
            missing_files = []
            
            for name, filenames in csv_filenames.items():
                success = False
                for filename in filenames:
                    path = os.path.join(csv_base_dir, filename)
                    if os.path.exists(path):
                        try:
                            # 添加错误处理逻辑以处理空文件或格式错误
                            file_size = os.path.getsize(path)
                            if file_size == 0:
                                print(f"Warning: {path} is empty, skipping.")
                                continue
                                
                            # 读取CSV文件
                            df = pd.read_csv(path)
                            
                            # 检查是否有timestamp列
                            if 'timestamp' not in df.columns:
                                print(f"Warning: {path} does not have a 'timestamp' column")
                                # 尝试找到第一列作为timestamp
                                if df.shape[1] > 0:
                                    print(f"Using first column as timestamp: {df.columns[0]}")
                                    df.rename(columns={df.columns[0]: 'timestamp'}, inplace=True)
                                else:
                                    print(f"Error: {path} has no columns")
                                    continue
                            
                            # 检查数据帧是否为空
                            if df.empty:
                                print(f"Warning: {path} is empty after reading")
                                continue
                                
                            # 检查数据帧的有效行数
                            valid_rows = df.dropna(subset=['timestamp']).shape[0]
                            if valid_rows == 0:
                                print(f"Warning: {path} has no valid rows with timestamps")
                                continue
                                
                            # 验证timestamp列的数据类型
                            df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
                            df = df.dropna(subset=['timestamp'])
                            
                            # 检查清理后是否还有数据
                            if df.empty:
                                print(f"Warning: {path} has no valid numeric timestamps")
                                continue
                                
                            # 确认所有数据列都被转换为数值类型
                            for col in df.columns:
                                if col != 'timestamp' and not col.startswith('Unnamed'):
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                            # 删除所有NaN值行
                            df_orig_len = len(df)
                            df = df.dropna()
                            if len(df) < df_orig_len:
                                print(f"Warning: Removed {df_orig_len - len(df)} rows with NaN values from {path}")
                            
                            if df.empty:
                                print(f"Warning: {path} has all NaN values after conversion")
                                continue
                            
                            # 存储处理后的数据帧
                            dataframes[name] = df
                            print(f"Read {name} from {filename}: {len(df)} rows")
                            # 打印CSV文件的列名，方便调试
                            print(f"Columns in {filename}: {list(df.columns)}")
                            success = True
                            break
                        except Exception as e:
                            print(f"Error reading {path}: {str(e)}")
                            traceback.print_exc()
                
                if not success:
                    print(f"Warning: Could not read {name} from any of these files: {filenames}")
                    missing_files.append(name)
            
            # 检查是否缺少任何必要的文件
            essential_files = ['left_arm_state', 'right_arm_state', 'left_arm_action', 'right_arm_action']
            essential_missing = [f for f in essential_files if f in missing_files]
            
            if essential_missing:
                print(f"Error: Missing essential files: {essential_missing}")
                print("Cannot proceed without arm state and action data")
                return False
            
            # 为缺失的非必要文件创建空数据帧，以便于后续处理
            for name in missing_files:
                if name not in essential_files:
                    print(f"Creating empty dataframe for missing file: {name}")
                    if 'gripper' in name:
                        # 创建只有timestamp和一个值列的空数据帧
                        if 'state' in name:
                            col_name = f"{name.split('_')[0]}_gripper_pos"
                        else:
                            col_name = f"{name.split('_')[0]}_gripper_exp"
                        dataframes[name] = pd.DataFrame({'timestamp': [], col_name: []})
                    else:
                        # 这种情况不应该发生，因为我们已经检查了必要的文件
                        pass
            
            # 尝试读取相机图像
            camera_files = {}
            has_camera_data = False
            
            for cam in ['left', 'right']:
                try:
                    camera_files[cam] = self._get_camera_files(cam)
                    print(f"Found {len(camera_files[cam])} images for {cam} camera")
                    has_camera_data = True
                except Exception as e:
                    print(f"Warning: Could not read {cam} camera files: {str(e)}")
                    camera_files[cam] = []
            
            # 确定参考时间戳
            if has_camera_data:
                # 使用相机数据作为参考
                valid_cams = [cam for cam, files in camera_files.items() if len(files) > 0]
                cam_frames = {cam: len(files) for cam, files in camera_files.items() if cam in valid_cams}
                
                ref_cam = min(cam_frames.items(), key=lambda x: x[1])[0]
                ref_frames = cam_frames[ref_cam]
                
                print(f"\nFrame counts: {cam_frames}")
                print(f"Using {ref_cam} camera as reference with {ref_frames} frames")
                
                # 对参考相机的时间戳进行排序
                ref_camera_files = sorted(camera_files[ref_cam], key=lambda x: x[1])[:ref_frames]
                ref_times = [time for _, time in ref_camera_files]
                
                # 同步图像 - 存储为有序的序列
                print("\nSynchronizing images...")
                for cam_name in valid_cams:
                    print(f"Processing {cam_name} camera...")
                    files = camera_files[cam_name]
                    for i, ref_time in enumerate(tqdm(ref_times)):
                        src_file, _ = self._find_closest_file(files, ref_time)
                        # 使用有序的文件名，确保按顺序排列
                        dst_name = f"{i:08d}_{ref_time:.6f}.jpg"
                        dst_path = os.path.join(self.sync_dir, f'camera_{cam_name}', dst_name)
                        shutil.copy2(src_file, dst_path)
            else:
                # 没有相机数据，使用CSV时间戳作为参考
                print("\nNo camera data found. Using CSV timestamps as reference.")
                # 限制帧数以避免处理过多数据
                max_frames = 500
                ref_times = sorted(dataframes['left_arm_state']['timestamp'].values[:max_frames])
                print(f"Using {len(ref_times)} frames from CSV timestamps")
            
            # 创建同步的数据结构
            print("\nSynchronizing CSV data...")
            synced_data = {
                'timestamp': [],
                'frame_indices': [],  # 添加帧索引
                'left_arm_state': [],
                'right_arm_state': [],
                'left_arm_action': [],
                'right_arm_action': [],
                'left_gripper_state': [],
                'right_gripper_state': [],
                'left_gripper_action': [],
                'right_gripper_action': []
            }
            
            # 动态检测CSV文件中的列名
            print("\nDetecting column names in CSV files...")
            
            # 获取所有可能的关节列名
            def get_joint_columns(df):
                """尝试从数据帧中找到6个关节列名"""
                # 移除timestamp和unnamed列
                cols = [c for c in df.columns if c != 'timestamp' and not c.startswith('Unnamed')]
                
                # 检查列名中是否包含已知的关节列名模式
                joint_patterns = [
                    # 左臂关节模式
                    ['left_arm_pos_1', 'left_arm_pos_2', 'left_arm_pos_3', 'left_arm_pos_4', 'left_arm_pos_5', 'left_arm_pos_6'],
                    ['left_arm_exp_1', 'left_arm_exp_2', 'left_arm_exp_3', 'left_arm_exp_4', 'left_arm_exp_5', 'left_arm_exp_6'],
                    # 右臂关节模式
                    ['right_arm_pos_1', 'right_arm_pos_2', 'right_arm_pos_3', 'right_arm_pos_4', 'right_arm_pos_5', 'right_arm_pos_6'],
                    ['right_arm_exp_1', 'right_arm_exp_2', 'right_arm_exp_3', 'right_arm_exp_4', 'right_arm_exp_5', 'right_arm_exp_6'],
                    # 如果文件名中带有joint，可能使用这种模式
                    ['joint1', 'joint2', 'joint3', 'joint4', 'joint5', 'joint6'],
                    ['j1', 'j2', 'j3', 'j4', 'j5', 'j6']
                ]
                
                # 检查是否有完整的关节模式匹配
                for pattern in joint_patterns:
                    if all(p in cols for p in pattern):
                        print(f"Found complete joint pattern: {pattern}")
                        return pattern
                
                # 如果没有找到完整模式，尝试查找带有序号的列
                numbered_cols = []
                for col in cols:
                    # 查找形如 xxx_1, xxx_2 或 xxx1, xxx2 的列
                    if col[-2:].startswith('_') and col[-1].isdigit():
                        numbered_cols.append(col)
                    elif col[-1].isdigit() and (col[-2].isalpha() or col[-2] == '_'):
                        numbered_cols.append(col)
                
                # 对带序号的列排序
                if numbered_cols:
                    # 提取序号并排序
                    numbered_cols.sort(key=lambda x: int(x[-1]) if x[-1].isdigit() else int(x.split('_')[-1]))
                    if len(numbered_cols) >= 6:
                        print(f"Using numbered columns: {numbered_cols[:6]}")
                        return numbered_cols[:6]
                
                # 如果以上方法都失败，但至少有6列，使用前6列
                if len(cols) >= 6:
                    print(f"Using first 6 columns: {cols[:6]}")
                    return cols[:6]
                else:
                    # 如果列数不足，使用可用列并用0填充
                    print(f"Warning: Only found {len(cols)} columns, expected 6. Will use zeros to fill missing columns.")
                    return cols
            
            # 获取夹爪列名
            def get_gripper_column(df):
                """尝试从数据帧中找到夹爪列名"""
                # 移除timestamp和unnamed列
                cols = [c for c in df.columns if c != 'timestamp' and not c.startswith('Unnamed')]
                
                # 检查是否有已知的夹爪列名模式
                gripper_patterns = [
                    # 左夹爪模式
                    'left_gripper_pos',
                    'left_gripper_exp',
                    'left_gripper_position',
                    'left_gripper',
                    # 右夹爪模式
                    'right_gripper_pos',
                    'right_gripper_exp',
                    'right_gripper_position',
                    'right_gripper'
                ]
                
                # 首先尝试精确匹配
                for pattern in gripper_patterns:
                    if pattern in cols:
                        print(f"Found exact gripper column match: {pattern}")
                        return pattern
                
                # 然后尝试部分匹配
                for col in cols:
                    for pattern in gripper_patterns:
                        if pattern in col:
                            print(f"Found partial gripper column match: {col} (matches {pattern})")
                            return col
                
                # 如果没有匹配，但至少有一列，使用第一列
                if cols:
                    print(f"No gripper column matched, using first available column: {cols[0]}")
                    return cols[0]
                else:
                    print("Warning: No valid gripper column found.")
                    # 返回None表示没有找到有效列
                    return None

            # 检测并保存列名映射
            joint_columns = {}
            for name in ['left_arm_state', 'right_arm_state', 'left_arm_action', 'right_arm_action']:
                columns = get_joint_columns(dataframes[name])
                joint_columns[name] = columns
                print(f"Using columns for {name}: {columns}")
            
            gripper_columns = {}
            for name in ['left_gripper_state', 'right_gripper_state', 'left_gripper_action', 'right_gripper_action']:
                column = get_gripper_column(dataframes[name])
                gripper_columns[name] = column
                print(f"Using column for {name}: {column}")
            
            # 同步所有CSV数据 - 添加帧索引与时间戳的对应关系
            print("\n开始为每个时间戳提取数据...")
            
            # 在开始同步前，对所有数据帧进行排序和去重
            for key in dataframes:
                df = dataframes[key]
                if not df.empty:
                    # 按时间戳排序
                    df = df.sort_values('timestamp')
                    # 如果有重复时间戳，保留最后一个（最新的）
                    df = df.drop_duplicates(subset=['timestamp'], keep='last')
                    dataframes[key] = df
                    print(f"整理后的 {key} 数据有 {len(df)} 行")
            
            # 调试输出各数据帧的信息
            for name in joint_columns.keys():
                df = dataframes[name]
                if not df.empty:
                    print(f"\n{name} 数据测试：")
                    print(f"行数: {len(df)}")
                    print(f"前5个时间戳: {df['timestamp'].values[:5] if len(df) >= 5 else df['timestamp'].values}")
                    print(f"前5个位置值: {df.iloc[:5][joint_columns[name]].values if len(df) >= 5 else df[joint_columns[name]].values}")
                    if len(df) >= 10:
                        print(f"随机5个位置值 (中间): {df.iloc[len(df)//2:len(df)//2+5][joint_columns[name]].values}")
                        print(f"后5个位置值: {df.iloc[-5:][joint_columns[name]].values}")
            
            # 记录原始数据信息，用于验证是否正确同步
            total_unique_positions = {}
            for name in joint_columns.keys():
                df = dataframes[name]
                if not df.empty:
                    positions = df[joint_columns[name]].values
                    unique_count = len(np.unique(positions, axis=0))
                    total_unique_positions[name] = unique_count
                    print(f"{name} 有 {unique_count} 个不同的位置值")
                else:
                    total_unique_positions[name] = 0
                    print(f"{name} 数据为空")
            
            # 创建一个查找最近数据点的函数
            def find_nearest_data(df, timestamp, columns):
                """
                安全地从数据帧中获取指定时间戳附近的数据，使用最近邻而不是插值。
                如果时间戳超出范围，则使用最近的有效值。
                """
                if df.empty:
                    return [0.0] * len(columns)
                
                # 转换为numpy数组，提高效率
                all_timestamps = df['timestamp'].values
                all_data = df[columns].values
                
                # 检查时间戳是否在范围内
                if timestamp <= all_timestamps[0]:
                    # 如果小于最小时间戳，使用第一行数据
                    return all_data[0].tolist()
                elif timestamp >= all_timestamps[-1]:
                    # 如果大于最大时间戳，使用最后一行数据
                    return all_data[-1].tolist()
                
                # 使用二分查找找到最近的时间戳
                idx = np.searchsorted(all_timestamps, timestamp)
                
                # 确定哪个时间点更接近
                if idx == 0:
                    closest_idx = 0
                elif idx == len(all_timestamps):
                    closest_idx = len(all_timestamps) - 1
                else:
                    # 比较idx和idx-1哪个更接近timestamp
                    if abs(all_timestamps[idx] - timestamp) <= abs(all_timestamps[idx-1] - timestamp):
                        closest_idx = idx
                    else:
                        closest_idx = idx - 1
                
                # 返回最接近的数据点，不进行插值
                return all_data[closest_idx].tolist()
            
            for i, ref_time in enumerate(tqdm(ref_times)):
                synced_data['timestamp'].append(ref_time)
                synced_data['frame_indices'].append(i)
                
                # 为每个数据源找到最近的数据点
                for name in joint_columns.keys():
                    if dataframes[name].empty:
                        synced_data[name].append([0.0] * len(joint_columns[name]))
                    else:
                        values = find_nearest_data(dataframes[name], ref_time, joint_columns[name])
                        synced_data[name].append(values)
                
                # 处理夹爪数据
                for name in gripper_columns.keys():
                    if gripper_columns[name] is None or dataframes[name].empty:
                        synced_data[name].append([0.0])
                    else:
                        values = find_nearest_data(dataframes[name], ref_time, [gripper_columns[name]])
                        synced_data[name].append(values)
            
            # 验证同步数据
            synced_unique_positions = {}
            for name in joint_columns.keys():
                positions = np.array(synced_data[name])
                unique_count = len(np.unique(positions, axis=0))
                synced_unique_positions[name] = unique_count
                print(f"同步后的 {name} 有 {unique_count} 个不同的位置值")
                
                # 检查NaN值的情况
                nan_count = np.isnan(positions).sum()
                if nan_count > 0:
                    print(f"警告: {name} 中有 {nan_count} 个NaN值!")
                
                # 检查零值的情况
                zero_rows = np.all(positions == 0, axis=1).sum()
                if zero_rows > 0:
                    print(f"信息: {name} 中有 {zero_rows} 行全为零值")
            
            # 保存同步数据
            print("\n保存同步数据...")
            pickle_path = os.path.join(self.sync_dir, 'synced_data.pickle')
            with open(pickle_path, 'wb') as f:
                pickle.dump(synced_data, f)
            
            print(f"同步完成，处理了 {len(ref_times)} 个时间点")
            print(f"原始不同位置数: {total_unique_positions}")
            print(f"同步后不同位置数: {synced_unique_positions}")
            
            return True
            
        except Exception as e:
            print(f"\nError in synchronization: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def step2_convert_to_h5(self) -> bool:
        """将同步的数据转换为H5格式，合并左右臂数据。"""
        try:
            print("\nStep 2: Converting to H5 format with combined arm data")
            
            # 加载同步的数据
            pickle_path = os.path.join(self.sync_dir, 'synced_data.pickle')
            if not os.path.exists(pickle_path):
                print(f"Error: Synchronized data file not found at {pickle_path}")
                return False
                
            with open(pickle_path, 'rb') as f:
                synced_data = pickle.load(f)
            
            h5_path = os.path.join(self.sync_dir, 'final_synchronized.h5')
            n_samples = len(synced_data['timestamp'])
            
            print(f"Creating H5 file with {n_samples} samples")
            
            with h5py.File(h5_path, 'w') as h5file:
                # 1. 时间戳 - 只保存时间戳，不存储帧索引
                timestamps = np.array(synced_data['timestamp'])
                
                # 检查时间戳格式 - 如果是17开头的纳秒时间戳(2023/2024年的数据)，则不需要乘以10^9
                first_timestamp = timestamps[0] if len(timestamps) > 0 else 0
                timestamp_str = str(int(first_timestamp))
                
                # 判断时间戳格式
                if timestamp_str.startswith('17') and len(timestamp_str) >= 18:
                    # 已经是纳秒级时间戳，无需转换
                    print(f"检测到纳秒级时间戳: {first_timestamp}, 长度: {len(timestamp_str)}")
                    h5_timestamps = timestamps.astype(np.int64)
                else:
                    # 如果是秒级时间戳，需要转换为纳秒
                    print(f"将秒级时间戳转换为纳秒: {first_timestamp}")
                    h5_timestamps = (timestamps * 1000000000).astype(np.int64)  # 转换为纳秒
                
                print(f"H5文件中的第一个时间戳: {h5_timestamps[0]}")
                h5file.create_dataset('timestamp', data=h5_timestamps, dtype=np.int64)
                
                # 不再将帧索引保存到H5文件中，但在内部处理中仍然使用它们
                # frame_indices保留在synced_data中，但不写入H5文件
                
                # 2. 创建与原始数据相同的组结构
                action_group = h5file.create_group('action')
                state_group = h5file.create_group('state')
                
                # 创建子组
                for group_path in [
                    'action/effector', 'action/end', 'action/head', 'action/joint', 
                    'action/robot', 'action/waist', 'state/effector', 'state/end', 
                    'state/head', 'state/joint', 'state/robot', 'state/waist'
                ]:
                    h5file.create_group(group_path)
                
                # 3. 创建索引数据集
                print("Creating indices...")
                indices = np.arange(n_samples)
                for path in [
                    'action/effector/index', 'action/end/index', 'action/head/index',
                    'action/joint/index', 'action/robot/index', 'action/waist/index'
                ]:
                    h5file.create_dataset(path, data=indices, dtype=np.int64)
                
                # 4. 创建合并的关节状态和动作数据
                print("Creating combined joint data (12-dim: left 6 + right 6)...")
                left_arm_state = np.array(synced_data['left_arm_state'])
                right_arm_state = np.array(synced_data['right_arm_state'])
                left_arm_action = np.array(synced_data['left_arm_action'])
                right_arm_action = np.array(synced_data['right_arm_action'])
                
                # 输出调试信息
                print(f"Left arm state shape: {left_arm_state.shape}")
                print(f"Right arm state shape: {right_arm_state.shape}")
                print(f"Left arm action shape: {left_arm_action.shape}")
                print(f"Right arm action shape: {right_arm_action.shape}")
                
                # 验证数据的正确性
                unique_left_states = len(np.unique(left_arm_state, axis=0))
                unique_right_states = len(np.unique(right_arm_state, axis=0))
                unique_left_actions = len(np.unique(left_arm_action, axis=0))
                unique_right_actions = len(np.unique(right_arm_action, axis=0))
                
                print(f"Unique left arm states: {unique_left_states}")
                print(f"Unique right arm states: {unique_right_states}")
                print(f"Unique left arm actions: {unique_left_actions}")
                print(f"Unique right arm actions: {unique_right_actions}")
                
                # 检查NaN值并替换为0
                for arr_name, arr in [
                    ("left_arm_state", left_arm_state),
                    ("right_arm_state", right_arm_state),
                    ("left_arm_action", left_arm_action),
                    ("right_arm_action", right_arm_action)
                ]:
                    nan_count = np.isnan(arr).sum()
                    if nan_count > 0:
                        print(f"警告: {arr_name} 包含 {nan_count} 个NaN值.")
                    
                    # 检查无穷大值
                    inf_count = np.isinf(arr).sum()
                    if inf_count > 0:
                        print(f"警告: {arr_name} 包含 {inf_count} 个无穷大值.")
                
                # 合并左右手臂数据，维度为(n_samples, 12)
                combined_arm_state = np.hstack((left_arm_state, right_arm_state))
                combined_arm_action = np.hstack((left_arm_action, right_arm_action))
                
                print(f"Combined arm state shape: {combined_arm_state.shape}")
                print(f"Combined arm action shape: {combined_arm_action.shape}")
                print(f"Number of unique combined states: {len(np.unique(combined_arm_state, axis=0))}")
                print(f"Number of unique combined actions: {len(np.unique(combined_arm_action, axis=0))}")
                
                h5file.create_dataset('state/joint/position', data=combined_arm_state, dtype=np.float64)
                h5file.create_dataset('action/joint/position', data=combined_arm_action, dtype=np.float64)
                
                # 与原始数据结构一致，添加current_value字段
                h5file.create_dataset('state/joint/current_value', data=combined_arm_state, dtype=np.float64)
                
                # 5. 创建合并的夹爪状态和动作数据
                print("Creating combined gripper data (2-dim: left 1 + right 1)...")
                left_gripper_state = np.array(synced_data['left_gripper_state']).reshape(-1, 1)
                right_gripper_state = np.array(synced_data['right_gripper_state']).reshape(-1, 1)
                left_gripper_action = np.array(synced_data['left_gripper_action']).reshape(-1, 1)
                right_gripper_action = np.array(synced_data['right_gripper_action']).reshape(-1, 1)
                
                # 打印夹爪形状
                print(f"左夹爪状态形状: {left_gripper_state.shape}")
                print(f"右夹爪状态形状: {right_gripper_state.shape}")
                print(f"左夹爪动作形状: {left_gripper_action.shape}")
                print(f"右夹爪动作形状: {right_gripper_action.shape}")
                
                # 检查NaN值并替换为0
                for arr_name, arr in [
                    ("left_gripper_state", left_gripper_state),
                    ("right_gripper_state", right_gripper_state),
                    ("left_gripper_action", left_gripper_action),
                    ("right_gripper_action", right_gripper_action)
                ]:
                    nan_count = np.isnan(arr).sum()
                    if nan_count > 0:
                        print(f"警告: {arr_name} 包含 {nan_count} 个NaN值.")
                    
                    # 检查无穷大值
                    inf_count = np.isinf(arr).sum()
                    if inf_count > 0:
                        print(f"警告: {arr_name} 包含 {inf_count} 个无穷大值.")
                
                # 合并左右夹爪数据
                combined_gripper_state = np.hstack((left_gripper_state, right_gripper_state))
                combined_gripper_action = np.hstack((left_gripper_action, right_gripper_action))
                
                print(f"Combined gripper state shape: {combined_gripper_state.shape}")
                print(f"Combined gripper action shape: {combined_gripper_action.shape}")
                
                h5file.create_dataset('state/effector/position', data=combined_gripper_state, dtype=np.float64)
                h5file.create_dataset('action/effector/position', data=combined_gripper_action, dtype=np.float64)
                
                # 6. 添加空的数据集以匹配原始结构
                empty_datasets = {
                    'action/effector/force': (0,),
                    'action/end/orientation': (0,),
                    'action/end/position': (0,),
                    'action/head/position': (0,),
                    'action/joint/effort': (0,),
                    'action/joint/velocity': (0,),
                    'action/robot/orientation': (0,),
                    'action/robot/position': (0,),
                    'action/robot/velocity': (0,),
                    'action/waist/position': (0,),
                    'state/effector/force': (0,),
                    'state/end/angular': (0,),
                    'state/end/orientation': (0,),
                    'state/end/position': (0,),
                    'state/end/velocity': (0,),
                    'state/end/wrench': (0,),
                    'state/head/effort': (0,),
                    'state/head/position': (0,),
                    'state/head/velocity': (0,),
                    'state/joint/effort': (0,),
                    'state/joint/velocity': (0,),
                    'state/robot/orientation': (0,),
                    'state/robot/orientation_drift': (0,),
                    'state/robot/position': (0,),
                    'state/robot/position_drift': (0,),
                    'state/waist/effort': (0,),
                    'state/waist/position': (0,),
                    'state/waist/velocity': (0,)
                }
                
                for path, shape in empty_datasets.items():
                    if 'effort' in path or 'force' in path or 'wrench' in path or 'angular' in path or 'drift' in path:
                        dtype = np.float32
                    else:
                        dtype = np.float64
                    h5file.create_dataset(path, shape, dtype=dtype)
                
                print(f"H5 file created: {h5_path}")
                return True
                
        except Exception as e:
            print(f"\nError in H5 conversion: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def step3_reorganize(self) -> bool:
        """重组数据结构：创建与原始数据完全一致的输出格式。"""
        try:
            print("\nStep 3: Reorganizing data structure")
            
            # 将H5文件复制到最终目录
            h5_src = os.path.join(self.sync_dir, 'final_synchronized.h5')
            h5_dst = os.path.join(self.final_output_dir, 'record', self.output_filename)
            
            if not os.path.exists(h5_src):
                print(f"Error: H5 file not found at {h5_src}")
                return False
                
            # 确保record目录存在
            os.makedirs(os.path.join(self.final_output_dir, 'record'), exist_ok=True)
            
            shutil.copy2(h5_src, h5_dst)
            print(f"Copied H5 file to {h5_dst}")
            
            # 获取同步数据以确保图像与数据保持一致
            pickle_path = os.path.join(self.sync_dir, 'synced_data.pickle')
            with open(pickle_path, 'rb') as f:
                synced_data = pickle.load(f)
            
            # 获取所有图像文件并按照索引排序
            image_files = {}
            for cam in ['left', 'right']:
                src_dir = os.path.join(self.sync_dir, f'camera_{cam}')
                image_files[cam] = []
                
                # 查找所有图像文件
                all_files = []
                for ext in ['jpg', 'jpeg', 'png']:
                    all_files.extend(glob.glob(os.path.join(src_dir, f"*.{ext}")))
                
                # 从文件名中提取索引和时间戳
                for file in all_files:
                    try:
                        basename = os.path.basename(file)
                        # 文件命名格式: "00000001_1741175128.365597.jpg"
                        parts = basename.split('_')
                        if len(parts) >= 2:
                            index = int(parts[0])
                            
                            # 处理时间戳部分时要考虑文件扩展名
                            time_parts = parts[1].split('.')
                            # 如果格式是 1741175128.365597.jpg，则有3个部分
                            if len(time_parts) >= 3:
                                # 时间戳的整数部分和小数部分
                                timestamp_str = time_parts[0] + '.' + time_parts[1]
                                timestamp = float(timestamp_str)
                            # 如果格式是 1741175128.jpg (没有小数部分)
                            else:
                                timestamp = float(time_parts[0])
                                
                            image_files[cam].append((file, index, timestamp))
                    except (ValueError, IndexError) as e:
                        print(f"Warning: Could not parse index/timestamp from {file}: {e}")
                
                # 按索引排序
                image_files[cam].sort(key=lambda x: x[1])
                
                if not image_files[cam]:
                    print(f"Warning: No image files found in {src_dir}")
            
            # 如果有图像文件，则创建从0开始编号的相机文件夹
            if any(image_files.values()):
                print(f"Creating camera folder structure with frame index starting from 0...")
                
                # 确保相机目录存在
                os.makedirs(os.path.join(self.final_output_dir, 'camera'), exist_ok=True)
                
                # 计算帧数
                frame_indices = synced_data.get('frame_indices', list(range(len(synced_data['timestamp']))))
                frame_count = len(frame_indices)
                
                print(f"Found {frame_count} synchronized frames")
                
                # 创建相机文件夹结构，从0开始编号
                for i in range(frame_count):
                    # 文件夹从0开始编号
                    folder_path = os.path.join(self.final_output_dir, 'camera', str(i))
                    os.makedirs(folder_path, exist_ok=True)
                    
                    # 复制左右相机图像，按索引查找
                    for cam in ['left', 'right']:
                        matching_files = [f for f in image_files[cam] if f[1] == i]
                        if matching_files:
                            src_file = matching_files[0][0]
                            dst_file = os.path.join(folder_path, f"cam_{cam}_color.jpg")
                            shutil.copy2(src_file, dst_file)
                        else:
                            print(f"Warning: Missing {cam} camera image for frame {i}")
                
                print(f"Created {frame_count} camera frame folders with indices from 0 to {frame_count-1}")
            
            print(f"Successfully reorganized data to {self.final_output_dir}")
            return True
            
        except Exception as e:
            print(f"\nError in reorganization: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def cleanup(self):
        """清理临时文件。"""
        try:
            # 删除实例特定的临时目录
            if os.path.exists(self.sync_base):
                shutil.rmtree(self.sync_base)
                print(f"Cleaned up temporary files at {self.sync_base}")
            
            # 尝试删除可能为空的temp_sync目录
            temp_sync_dir = os.path.dirname(self.sync_base)
            if os.path.exists(temp_sync_dir) and os.path.basename(temp_sync_dir) == "temp_sync":
                if not os.listdir(temp_sync_dir):
                    os.rmdir(temp_sync_dir)
                    print(f"Removed empty temp_sync directory at {temp_sync_dir}")
        except Exception as e:
            print(f"Warning: Failed to clean up temporary files: {str(e)}")

    def process(self) -> bool:
        """执行整个处理流程。"""
        try:
            print(f"\nStarting data processing for {self.data_dir}")
            
            # 步骤1：同步数据
            if not self.step1_synchronize():
                print("Error: Failed to synchronize data. Aborting.")
                return False
                
            # 步骤2：转换为H5
            if not self.step2_convert_to_h5():
                print("Error: Failed to convert data to H5. Aborting.")
                return False
                
            # 步骤3：重组数据
            if not self.step3_reorganize():
                print("Error: Failed to reorganize data. Aborting.")
                return False
                
            print(f"\nData processing completed successfully for {self.data_dir}")
            
            # 清理临时文件
            self.cleanup()
            
            return True
            
        except Exception as e:
            print(f"\nUnexpected error during processing: {str(e)}")
            traceback.print_exc()
            return False

def process_single_dir(d: str, output_dir: str, output_dirname: str = None):
    """处理单个数据目录。"""
    processor = CombinedDualArmDataProcessor(d, output_dir, output_dirname)
    success = processor.process()
    return success, d

def process_all_directories(base_dir: str, output_dir: str, output_dirname: str = None, max_workers: int = 3):
    """处理基础目录下的所有子目录。"""
    # 查找所有可能的数据目录
    possible_dirs = []
    
    # 如果基础目录包含images或csv，则直接处理
    has_data = (
        os.path.exists(os.path.join(base_dir, 'images')) or 
        os.path.exists(os.path.join(base_dir, 'csv')) or
        len(glob.glob(os.path.join(base_dir, '*.csv'))) > 0
    )
    
    if has_data:
        possible_dirs.append(base_dir)
    
    # 查找一级子目录
    for item in os.listdir(base_dir):
        d = os.path.join(base_dir, item)
        if not os.path.isdir(d):
            continue
            
        # 检查是否包含数据文件
        has_data = (
            os.path.exists(os.path.join(d, 'images')) or 
            os.path.exists(os.path.join(d, 'csv')) or
            len(glob.glob(os.path.join(d, '*.csv'))) > 0
        )
        
        if has_data:
            possible_dirs.append(d)
    
    if not possible_dirs:
        print(f"No valid data directories found in {base_dir}")
        return
        
    print(f"Found {len(possible_dirs)} directories to process")
    
    # 并行处理所有目录
    success_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dir = {executor.submit(process_single_dir, d, output_dir, output_dirname): d for d in possible_dirs}
        
        for future in as_completed(future_to_dir):
            d = future_to_dir[future]
            try:
                success, _ = future.result()
                if success:
                    success_count += 1
                    print(f"Successfully processed {d}")
                else:
                    print(f"Failed to process {d}")
            except Exception as e:
                print(f"Processing {d} raised an exception: {str(e)}")
    
    print(f"Processed {success_count}/{len(possible_dirs)} directories successfully")

def main():
    parser = argparse.ArgumentParser(description='Process dual-arm robot data with combined arm representation.')
    parser.add_argument('--base_dir', type=str, help='Base directory containing data directories')
    parser.add_argument('--output_dir', type=str, help='Output directory for processed data')
    parser.add_argument('--single_dir', type=str, help='Single directory to process')
    parser.add_argument('--output_dirname', type=str, default=None, help='Name of the output subdirectory (default: auto-generated)')
    parser.add_argument('--threads', type=int, default=3, help='Number of parallel processing threads')
    
    args = parser.parse_args()
    
    if not args.output_dir:
        print("Error: Output directory must be specified")
        return
        
    # 创建输出目录，使用try-except捕获权限错误
    try:
        os.makedirs(args.output_dir, exist_ok=True)
    except PermissionError:
        print(f"Error: No permission to create directory at {args.output_dir}")
        print("Please specify a directory where you have write permissions.")
        return
    except OSError as e:
        print(f"Error creating output directory: {e}")
        print("Please specify a valid directory path.")
        return
    
    if args.single_dir:
        # 处理单个目录
        print(f"Processing single directory: {args.single_dir}")
        processor = CombinedDualArmDataProcessor(args.single_dir, args.output_dir, args.output_dirname)
        processor.process()
    elif args.base_dir:
        # 处理基础目录下的所有子目录
        print(f"Processing all directories in: {args.base_dir}")
        process_all_directories(args.base_dir, args.output_dir, args.output_dirname, args.threads)
    else:
        print("Error: Either base_dir or single_dir must be specified")

if __name__ == "__main__":
    main() 