// static/script.js
document.addEventListener('DOMContentLoaded', function() {
    // 获取DOM元素
    const folderPathInput = document.getElementById('folderPath');
    folderPathInput.value = '/home/nvidia/GalaxeaDataset/data';
    const scanBtn = document.getElementById('scanBtn');
    const refreshBtn = document.getElementById('refreshBtn');
    const deleteBtn = document.getElementById('deleteBtn');
    const selectAllCheckbox = document.getElementById('selectAll');
    const progressBar = document.getElementById('progressBar');
    const scanningAlert = document.getElementById('scanningAlert');
    const errorAlert = document.getElementById('errorAlert');
    const resultsCard = document.getElementById('resultsCard');
    const bagsTableBody = document.getElementById('bagsTableBody');
    const totalFilesSpan = document.getElementById('totalFiles');
    const totalDurationSpan = document.getElementById('totalDuration');
    const totalSizeSpan = document.getElementById('totalSize');
    const emptyState = document.getElementById('emptyState');
    
    // 删除相关元素
    const deleteModal = new bootstrap.Modal(document.getElementById('deleteModal'));
    const deleteFilesList = document.getElementById('deleteFilesList');
    const confirmDeleteBtn = document.getElementById('confirmDeleteBtn');
    const deleteResultModal = new bootstrap.Modal(document.getElementById('deleteResultModal'));
    const deleteSuccessList = document.getElementById('deleteSuccessList');
    const deleteFailedList = document.getElementById('deleteFailedList');
    const deleteFailedDiv = document.getElementById('deleteFailedDiv');
    const deleteSuccessDiv = document.getElementById('deleteSuccessDiv');
    
    // 当前文件夹路径
    let currentFolderPath = '';
    // 当前bag文件列表
    let bagFiles = [];
    
    // 扫描按钮点击事件
    scanBtn.addEventListener('click', function() {
        const folderPath = folderPathInput.value.trim();
        if (folderPath) {
            scanFolder(folderPath);
        } else {
            showError('请输入有效的文件夹路径');
        }
    });
    
    // 回车键触发扫描
    folderPathInput.addEventListener('keyup', function(event) {
        if (event.key === 'Enter') {
            scanBtn.click();
        }
    });
    
    // 刷新按钮点击事件
    refreshBtn.addEventListener('click', function() {
        if (currentFolderPath) {
            scanFolder(currentFolderPath);
        }
    });
    
    // 全选/取消全选
    selectAllCheckbox.addEventListener('change', function() {
        const isChecked = this.checked;
        const checkboxes = document.querySelectorAll('.bag-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.checked = isChecked;
        });
        updateDeleteButtonState();
    });
    
    // 删除按钮点击事件
    deleteBtn.addEventListener('click', function() {
        const selectedFiles = getSelectedFiles();
        if (selectedFiles.length > 0) {
            showDeleteConfirmation(selectedFiles);
        }
    });
    
    // 确认删除按钮点击事件
    confirmDeleteBtn.addEventListener('click', function() {
        const selectedFiles = getSelectedFiles();
        if (selectedFiles.length > 0) {
            deleteFiles(selectedFiles);
            deleteModal.hide();
        }
    });
    
    // 扫描文件夹
    function scanFolder(folderPath) {
        // 显示加载状态
        showScanning(true);
        hideError();
        
        // 发送请求到后端
        fetch('/scan', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                folder_path: folderPath
            })
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || '扫描失败');
                });
            }
            return response.json();
        })
        .then(data => {
            // 更新当前路径
            currentFolderPath = folderPath;
            
            // 保存bag文件列表
            bagFiles = data.bags;
            
            // 更新UI
            updateBagsTable(data.bags);
            updateStatistics(data.stats);
            
            // 显示结果卡片
            if (!resultsCard.classList.contains('show')) {
                resultsCard.classList.remove('hidden');
                setTimeout(() => {
                    resultsCard.classList.add('show');
                }, 10);
            }
            
            // 隐藏加载状态
            showScanning(false);
        })
        .catch(error => {
            showError(error.message);
            showScanning(false);
        });
    }
    
    // 更新bag文件表格
    function updateBagsTable(bags) {
        // 清空表格
        bagsTableBody.innerHTML = '';
        
        if (bags.length === 0) {
            emptyState.classList.remove('hidden');
            return;
        } else {
            emptyState.classList.add('hidden');
        }
        
        // 添加新行
        bags.forEach((bag, index) => {
            const row = document.createElement('tr');
            
            // 格式化文件大小显示
            const formattedSize = formatFileSize(bag.size);
            
            row.innerHTML = `
                <td>
                    <div class="form-check">
                        <input class="form-check-input bag-checkbox" type="checkbox" data-index="${index}">
                    </div>
                </td>
                <td>
                    <div class="d-flex flex-column">
                        <span class="file-name">${bag.file_name}</span>
                        <span class="file-path">${bag.rel_path}</span>
                    </div>
                </td>
                <td>
                    <span class="file-duration">${bag.duration}</span>
                </td>
                <td>
                    <span class="file-size">${formattedSize}</span>
                </td>
            `;
            
            bagsTableBody.appendChild(row);
        });
        
        // 添加动画效果
        const rows = bagsTableBody.querySelectorAll('tr');
        rows.forEach((row, index) => {
            row.style.animation = `fadeIn 0.3s ease forwards ${index * 0.05}s`;
            row.style.opacity = 0;
        });
        
        // 添加复选框事件
        document.querySelectorAll('.bag-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                updateDeleteButtonState();
                updateSelectAllCheckbox();
            });
        });
        
        // 重置全选复选框
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
        
        // 更新删除按钮状态
        updateDeleteButtonState();
    }
    
    // 格式化文件大小显示
    function formatFileSize(sizeInMB) {
        if (sizeInMB >= 1024) {
            return (sizeInMB / 1024).toFixed(2) + ' GB';
        } else {
            return sizeInMB.toFixed(2) + ' MB';
        }
    }
    
    // 更新统计信息
    function updateStatistics(stats) {
        // 添加动画效果
        totalFilesSpan.textContent = '0';
        totalDurationSpan.textContent = '00:00:00';
        totalSizeSpan.textContent = '0';
        
        // 使用定时器创建计数效果
        const duration = 1000; // 1秒动画
        const steps = 20; // 20步完成
        const fileIncrement = stats.total_files / steps;
        const sizeIncrement = stats.total_size / steps;
        let currentStep = 0;
        
        const interval = setInterval(() => {
            currentStep++;
            
            if (currentStep >= steps) {
                totalFilesSpan.textContent = stats.total_files;
                totalDurationSpan.textContent = stats.total_duration;
                totalSizeSpan.textContent = stats.total_size.toFixed(2);
                clearInterval(interval);
            } else {
                totalFilesSpan.textContent = Math.floor(fileIncrement * currentStep);
                totalSizeSpan.textContent = (sizeIncrement * currentStep).toFixed(2);
                
                // 时长不做动画，直接在最后一步显示
                if (currentStep === steps - 1) {
                    totalDurationSpan.textContent = stats.total_duration;
                }
            }
        }, duration / steps);
    }
    
    // 获取选中的文件
    function getSelectedFiles() {
        const selectedFiles = [];
        document.querySelectorAll('.bag-checkbox:checked').forEach(checkbox => {
            const index = parseInt(checkbox.getAttribute('data-index'));
            if (!isNaN(index) && index >= 0 && index < bagFiles.length) {
                selectedFiles.push(bagFiles[index]);
            }
        });
        return selectedFiles;
    }
    
    // 更新删除按钮状态
    function updateDeleteButtonState() {
        const selectedCount = document.querySelectorAll('.bag-checkbox:checked').length;
        if (selectedCount > 0) {
            deleteBtn.classList.remove('hidden');
            deleteBtn.innerHTML = `<i class="fas fa-trash me-1"></i> 删除所选 (${selectedCount})`;
        } else {
            deleteBtn.classList.add('hidden');
        }
    }
    
    // 更新全选复选框状态
    function updateSelectAllCheckbox() {
        const totalCheckboxes = document.querySelectorAll('.bag-checkbox').length;
        const checkedCheckboxes = document.querySelectorAll('.bag-checkbox:checked').length;
        
        if (checkedCheckboxes === 0) {
            selectAllCheckbox.checked = false;
            selectAllCheckbox.indeterminate = false;
        } else if (checkedCheckboxes === totalCheckboxes) {
            selectAllCheckbox.checked = true;
            selectAllCheckbox.indeterminate = false;
        } else {
            selectAllCheckbox.indeterminate = true;
        }
    }
    
    // 显示删除确认对话框
    function showDeleteConfirmation(files) {
        // 清空列表
        deleteFilesList.innerHTML = '';
        
        // 添加文件
        files.forEach(file => {
            const li = document.createElement('li');
            li.innerHTML = `
                <div class="d-flex align-items-center py-1">
                    <i class="fas fa-file-alt me-2 text-muted"></i>
                    <div>
                        <div class="fw-medium">${file.file_name}</div>
                        <div class="text-muted small">${file.rel_path}</div>
                    </div>
                </div>
            `;
            deleteFilesList.appendChild(li);
        });
        
        // 显示模态框
        deleteModal.show();
    }
    
    // 删除文件
    function deleteFiles(files) {
        // 获取文件路径列表
        const filePaths = files.map(file => file.path);
        
        // 显示加载状态
        showScanning(true);
        
        // 发送删除请求
        fetch('/delete', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                files: filePaths
            })
        })
        .then(response => response.json())
        .then(result => {
            // 隐藏加载状态
            showScanning(false);
            
            showDeleteResult(result);
            
            // 如果有文件成功删除，刷新列表
            if (result.success.length > 0) {
                setTimeout(() => {
                    scanFolder(currentFolderPath);
                }, 1000);
            }
        })
        .catch(error => {
            showScanning(false);
            showError('删除文件时出错: ' + error.message);
        });
    }
    
    // 显示删除结果
    function showDeleteResult(result) {
        // 清空列表
        deleteSuccessList.innerHTML = '';
        deleteFailedList.innerHTML = '';
        
        // 添加成功删除的文件
        if (result.success.length > 0) {
            deleteSuccessDiv.classList.remove('hidden');
            result.success.forEach(file => {
                const li = document.createElement('li');
                li.innerHTML = `
                    <div class="d-flex align-items-center">
                        <i class="fas fa-check-circle text-success me-2"></i>
                        <span>${file}</span>
                    </div>
                `;
                deleteSuccessList.appendChild(li);
            });
        } else {
            deleteSuccessDiv.classList.add('hidden');
        }
        
        // 添加删除失败的文件
        if (result.failed.length > 0) {
            deleteFailedDiv.classList.remove('hidden');
            result.failed.forEach(item => {
                const li = document.createElement('li');
                li.innerHTML = `
                    <div class="d-flex align-items-center">
                        <i class="fas fa-times-circle text-danger me-2"></i>
                        <div>
                            <div>${item.file}</div>
                            <div class="text-muted small">${item.reason}</div>
                        </div>
                    </div>
                `;
                deleteFailedList.appendChild(li);
            });
        } else {
            deleteFailedDiv.classList.add('hidden');
        }
        
        // 显示结果模态框
        deleteResultModal.show();
    }
    
    // 显示/隐藏扫描中状态
    function showScanning(isScanning) {
        if (isScanning) {
            progressBar.classList.remove('hidden');
            scanningAlert.classList.remove('hidden');
            scanBtn.disabled = true;
            refreshBtn.disabled = true;
        } else {
            progressBar.classList.add('hidden');
            scanningAlert.classList.add('hidden');
            scanBtn.disabled = false;
            refreshBtn.disabled = false;
        }
    }
    
    // 显示错误信息
    function showError(message) {
        errorAlert.innerHTML = `<i class="fas fa-exclamation-circle me-2"></i> ${message}`;
        errorAlert.classList.remove('hidden');
    }
    
    // 隐藏错误信息
    function hideError() {
        errorAlert.classList.add('hidden');
    }
});