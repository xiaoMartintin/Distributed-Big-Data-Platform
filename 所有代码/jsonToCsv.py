import json
import csv


with open('final_data.json', 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)


fields = ['modelId', 'lastModified', 'pipeline_tag', 'author', 'id', 'likes', 'downloads', 'library_name', 'datasets', 'architectures']

with open('final.csv', 'w', encoding='utf-8-sig', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fields)

    
    writer.writeheader()

    
    for model in data:
        
        datasets_str = 'unknown'
        if isinstance(model, dict) and 'cardData' in model and isinstance(model['cardData'], dict):
            datasets_list = model['cardData'].get('datasets', [])
            if datasets_list:  
                datasets_str = ','.join(map(str, datasets_list))

        architectures_str = 'unknown'
        if isinstance(model, dict) and 'config' in model and isinstance(model['config'], dict):
            architectures_list = model['config'].get('architectures', [])
            if architectures_list:  
                architectures_str = ','.join(map(str, architectures_list))  

        
        model_info = {
            "modelId": model.get('modelId', 'unknown'),
            "lastModified": model.get('lastModified', 'unknown'),
            "pipeline_tag": model.get('pipeline_tag', 'unknown'),
            "author": model.get('author', 'unknown'),  
            "id": model.get('_id', 'unknown'),  
            "likes": model.get('likes', 'unknown'),
            "downloads": model.get('downloads', 0),
            "library_name": model.get('library_name', 'unknown'),
            "datasets": datasets_str,
            "architectures": architectures_str
        }

        # 写入模型信息到CSV文件
        writer.writerow(model_info)

print("CSV文件已生成。")
