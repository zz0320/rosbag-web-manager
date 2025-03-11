# app.py - Flask应用主文件
import os
import re
import json
import shutil
import subprocess
from datetime import timedelta
from flask import Flask, render_template, request, jsonify, send_from_directory

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024  # 1GB限制

def get_bag_duration(bag_path):
    """获取bag文件的时长"""
    try:
        # 使用rosbag info命令获取bag信息
        result = subprocess.run(['rosbag', 'info', bag_path], 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.PIPE,
                               universal_newlines=True)
        
        output = result.stdout
        
        # 使用正则表达式匹配duration字段
        duration_match = re.search(r'duration:\s*(\d+\.\d+)s', output)
        if duration_match:
            duration_seconds = float(duration_match.group(1))
            # 转换为时分秒格式
            duration_str = str(timedelta(seconds=round(duration_seconds)))
            return duration_str
        else:
            return "未知"
            
    except Exception as e:
        print(f"获取bag时长出错: {e}")
        return "错误"

def scan_bags(folder_path):
    """扫描文件夹中的所有bag文件"""
    bag_files = []
    
    try:
        # 递归查找所有.bag文件
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.bag'):
                    full_path = os.path.join(root, file)
                    duration = get_bag_duration(full_path)
                    size = os.path.getsize(full_path)
                    # 转换为MB并保留两位小数
                    size_mb = round(size / (1024 * 1024), 2)
                    
                    # 获取相对路径
                    rel_path = os.path.relpath(full_path, folder_path)
                    
                    bag_files.append({
                        'file_name': file,
                        'path': full_path,
                        'rel_path': rel_path,
                        'duration': duration,
                        'size': size_mb
                    })
    except Exception as e:
        print(f"扫描文件夹出错: {e}")
    
    return bag_files

@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')

@app.route('/scan', methods=['POST'])
def scan_folder():
    """扫描指定文件夹"""
    data = request.get_json()
    folder_path = data.get('folder_path', '')
    
    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({'error': '无效的文件夹路径'}), 400
    
    bag_files = scan_bags(folder_path)
    
    # 计算统计信息
    total_files = len(bag_files)
    total_size = sum(bag['size'] for bag in bag_files)
    
    # 计算总时长
    total_seconds = 0
    for bag in bag_files:
        duration = bag['duration']
        if duration != "未知" and duration != "错误":
            try:
                # 解析时长字符串 (HH:MM:SS)
                parts = duration.split(':')
                if len(parts) == 3:
                    hours, minutes, seconds = map(int, parts)
                    total_seconds += hours * 3600 + minutes * 60 + seconds
            except:
                pass
    
    total_duration = str(timedelta(seconds=total_seconds))
    
    return jsonify({
        'bags': bag_files,
        'stats': {
            'total_files': total_files,
            'total_duration': total_duration,
            'total_size': round(total_size, 2)
        }
    })

@app.route('/delete', methods=['POST'])
def delete_files():
    """删除指定的bag文件"""
    data = request.get_json()
    files_to_delete = data.get('files', [])
    
    if not files_to_delete:
        return jsonify({'error': '未指定要删除的文件'}), 400
    
    results = {
        'success': [],
        'failed': []
    }
    
    for file_path in files_to_delete:
        try:
            if os.path.exists(file_path) and os.path.isfile(file_path):
                os.remove(file_path)
                results['success'].append(os.path.basename(file_path))
            else:
                results['failed'].append({
                    'file': os.path.basename(file_path),
                    'reason': '文件不存在'
                })
        except Exception as e:
            results['failed'].append({
                'file': os.path.basename(file_path),
                'reason': str(e)
            })
    
    return jsonify(results)

@app.route('/static/<path:path>')
def serve_static(path):
    """提供静态文件"""
    return send_from_directory('static', path)

if __name__ == '__main__':
    # 确保templates和static文件夹存在
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    # 运行应用
    app.run(debug=True, host='0.0.0.0', port=5000)