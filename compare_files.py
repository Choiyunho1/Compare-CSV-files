import pandas as pd
import os
import requests
import json

def safe_str_compare(val1, val2):
    """안전한 문자열 비교 함수"""
    # NaN이나 None 값을 빈 문자열로 처리
    val1 = '' if pd.isna(val1) else str(val1).strip()
    val2 = '' if pd.isna(val2) else str(val2).strip()
    
    # 특수문자가 포함된 경우 정확한 비교를 위해 원본 값 유지
    return val1 != val2

def format_value_for_display(value):
    """값을 표시하기 위한 형식으로 변환"""
    if pd.isna(value):
        return ''
    value = str(value).strip()
    # 특수문자가 포함된 경우 원본 그대로 표시
    return value

def compare_csv_files(old_file, new_file, output_file):
    # CSV 파일 읽기 (구분자 자동 감지 및 인코딩 처리)
    try:
        # 다양한 인코딩과 옵션으로 시도
        encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr']
        for encoding in encodings:
            try:
                df_old = pd.read_csv(old_file, 
                                   encoding=encoding,
                                   sep=None,
                                   engine='python',
                                   quoting=1,  # QUOTE_ALL
                                   on_bad_lines='skip',  # 문제가 있는 줄은 건너뛰기
                                   na_values=['', 'NA', 'NULL', 'null', 'None', 'none'],  # 빈 값 처리
                                   keep_default_na=True)  # 기본 NA 값도 유지
                df_new = pd.read_csv(new_file,
                                   encoding=encoding,
                                   sep=None,
                                   engine='python',
                                   quoting=1,  # QUOTE_ALL
                                   on_bad_lines='skip',  # 문제가 있는 줄은 건너뛰기
                                   na_values=['', 'NA', 'NULL', 'null', 'None', 'none'],  # 빈 값 처리
                                   keep_default_na=True)  # 기본 NA 값도 유지
                print(f"성공적으로 파일을 읽었습니다. 사용된 인코딩: {encoding}")
                break
            except Exception as e:
                print(f"{encoding} 인코딩으로 읽기 실패: {str(e)}")
                continue
        else:
            print("모든 인코딩 시도 실패")
            return
    except Exception as e:
        print(f"CSV 파일 읽기 실패: {str(e)}")
        return
    
    # 컬럼명 확인 및 출력
    print("기존 파일 컬럼:", df_old.columns.tolist())
    print("새 파일 컬럼:", df_new.columns.tolist())
    
    # ID와 PlatformName을 기준으로 비교
    # 새로운 항목 찾기
    new_items = df_new[~df_new[['ID', 'PlatformName']].apply(tuple, axis=1).isin(
        df_old[['ID', 'PlatformName']].apply(tuple, axis=1)
    )]
    
    # 변경된 항목 찾기
    merged_df = pd.merge(df_old, df_new, on=['ID', 'PlatformName'], suffixes=('_old', '_new'))
    
    # 변경된 컬럼 찾기
    changed_columns = []
    for col in df_old.columns:
        if col not in ['ID', 'PlatformName']:
            old_col = f"{col}_old"
            new_col = f"{col}_new"
            if old_col in merged_df.columns and new_col in merged_df.columns:
                # 안전한 비교를 위해 apply 사용
                changed = merged_df[merged_df.apply(lambda x: safe_str_compare(x[old_col], x[new_col]), axis=1)]
                if not changed.empty:
                    changed_columns.append({
                        'ID': changed['ID'],
                        'PlatformName': changed['PlatformName'],
                        'Column': col,
                        'Old_Value': changed[old_col].apply(format_value_for_display),
                        'New_Value': changed[new_col].apply(format_value_for_display)
                    })
    
    # 결과를 DataFrame으로 변환
    # 새로운 항목의 모든 컬럼 정보 포함
    new_items_details = []
    for _, row in new_items.iterrows():
        details = []
        for col in new_items.columns:
            if col not in ['ID', 'PlatformName']:
                value = format_value_for_display(row[col])
                details.append(f"{col}: {value}")
        new_items_details.append(" | ".join(details))
    
    new_items_df = pd.DataFrame({
        'Type': ['New Item'] * len(new_items),
        'ID': new_items['ID'],
        'PlatformName': new_items['PlatformName'],
        'Details': new_items_details
    })
    
    changed_items_df = pd.DataFrame()
    for change in changed_columns:
        temp_df = pd.DataFrame({
            'Type': ['Changed Value'] * len(change['ID']),
            'ID': change['ID'],
            'PlatformName': change['PlatformName'],
            'Details': [f"{change['Column']} changed from '{old}' to '{new}'" 
                       for old, new in zip(change['Old_Value'], change['New_Value'])]
        })
        changed_items_df = pd.concat([changed_items_df, temp_df])
    
    # 최종 결과 DataFrame 생성
    result_df = pd.concat([new_items_df, changed_items_df])
    
    # 결과를 CSV 파일로 저장 (특수 문자 처리)
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig', quoting=1)
    
    # 빈 JSON 파일 생성
    json_output_file = output_file.replace('.csv', '.json')
    with open(json_output_file, 'w', encoding='utf-8') as f:
        f.write('{}')
    
    # JSON 파일을 로컬 서버로 업로드
    try:
        with open(json_output_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        response = requests.post('http://127.0.0.1/content-metadata.json', 
                               json=json_data,
                               headers={'Content-Type': 'application/json'})
        
        if response.status_code == 200:
            print(f"JSON 파일이 성공적으로 업로드되었습니다.")
        else:
            print(f"JSON 파일 업로드 실패. 상태 코드: {response.status_code}")
    except Exception as e:
        print(f"JSON 파일 업로드 중 오류 발생: {str(e)}")
    
    print(f"\n비교 결과가 {output_file}에 저장되었습니다.")
    print(f"JSON 파일이 {json_output_file}에 생성되었습니다.")
    print(f"새로운 항목 수: {len(new_items)}")
    print(f"변경된 항목 수: {len(changed_items_df)}")

if __name__ == "__main__":
    # 파일 경로 설정
    old_file = "5.0.47_PlacformDectect.csv"
    new_file = "5.0.65_PlacformDectect.csv"
    output_file = "comparison_result.csv"
    
    # 파일 존재 여부 확인
    if not os.path.exists(old_file):
        print(f"Error: {old_file} 파일을 찾을 수 없습니다.")
    elif not os.path.exists(new_file):
        print(f"Error: {new_file} 파일을 찾을 수 없습니다.")
    else:
        compare_csv_files(old_file, new_file, output_file)
