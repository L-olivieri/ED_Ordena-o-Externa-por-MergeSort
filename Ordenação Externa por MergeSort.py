import csv
import os
import tempfile
from heapq import merge
from itertools import islice

"""""
para usar 

external_sort(
    input_file='grande_arquivo.csv',
    output_file='arquivo_ordenado.csv',
    key_column='id',  # ou 0 para primeira coluna
    ascending=True,
    buffer_size=10000  # ajuste conforme sua RAM
)
"""



def external_sort(input_file, output_file, key_column, ascending=True, buffer_size=1000):
    """
    Ordena um arquivo CSV externamente usando merge-sort.
    
    Args:
        input_file (str): Caminho do arquivo CSV de entrada.
        output_file (str): Caminho do arquivo CSV de saída ordenado.
        key_column (str/int): Nome ou índice da coluna chave para ordenação.
        ascending (bool): Ordem crescente (True) ou decrescente (False).
        buffer_size (int): Número de linhas a serem carregadas na memória por vez.
    """
    # Fase 1: Dividir o arquivo em runs ordenados
    temp_runs = split_and_sort(input_file, key_column, ascending, buffer_size)
    
    # Fase 2: Mesclar os runs temporários
    merge_runs(temp_runs, output_file, key_column, ascending)
    
    # Limpeza: Remover arquivos temporários
    for run in temp_runs:
        os.remove(run)

def split_and_sort(input_file, key_column, ascending, buffer_size):
    """Divide o arquivo em chunks ordenados e retorna lista de arquivos temporários."""
    temp_runs = []
    
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Pula o cabeçalho
        
        # Determina se key_column é nome ou índice
        try:
            key_index = int(key_column)
        except ValueError:
            key_index = header.index(key_column)
        
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= buffer_size:
                # Ordena e salva o chunk
                temp_runs.append(save_sorted_chunk(chunk, key_index, ascending))
                chunk = []
        
        # Salva o último chunk (se houver)
        if chunk:
            temp_runs.append(save_sorted_chunk(chunk, key_index, ascending))
    
    return temp_runs

def save_sorted_chunk(chunk, key_index, ascending):
    """Ordena um chunk em memória e salva em arquivo temporário."""
    # Ordena pelo key_index (como string para evitar erro de tipo)
    reverse = not ascending
    chunk.sort(key=lambda x: x[key_index], reverse=reverse)
    
    # Salva em arquivo temporário
    fd, path = tempfile.mkstemp(suffix='.csv')
    with os.fdopen(fd, 'w', newline='') as tmpfile:
        writer = csv.writer(tmpfile)
        writer.writerows(chunk)
    
    return path

def merge_runs(run_files, output_file, key_index, ascending):
    """Mescla os arquivos temporários em um único arquivo ordenado."""
    # Abre todos os runs como leitores CSV
    readers = []
    for run in run_files:
        f = open(run, 'r')
        reader = csv.reader(f)
        readers.append((reader, f))
    
    # Determina se key_column é nome ou índice
    try:
        key_index = int(key_index)
    except ValueError:
        # Se for nome, pega do primeiro arquivo (já que todos têm o mesmo header)
        with open(run_files[0], 'r') as f:
            header = next(csv.reader(f))
            key_index = header.index(key_index)
    
    # Função para pegar a próxima linha de cada reader
    def get_next(reader):
        try:
            return next(reader)
        except StopIteration:
            return None
    
    # Inicializa a fila de prioridade (heap)
    heap = []
    for i, (reader, _) in enumerate(readers):
        row = get_next(reader)
        if row is not None:
            heap.append((row[key_index], i, row))
    
    # Configura a ordem do heap
    reverse = not ascending
    heap.sort(key=lambda x: x[0], reverse=reverse)
    
    # Abre o arquivo de saída
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        
        # Escreve o cabeçalho
        if run_files:
            with open(run_files[0], 'r') as f:
                header = next(csv.reader(f))
                writer.writerow(header)
        
        # Mescla os runs
        while heap:
            _, reader_idx, row = heap.pop(0)
            writer.writerow(row)
            
            # Pega próxima linha do reader que acabamos de usar
            next_row = get_next(readers[reader_idx][0])
            if next_row is not None:
                heap.append((next_row[key_index], reader_idx, next_row))
                heap.sort(key=lambda x: x[0], reverse=reverse)
    
    # Fecha todos os arquivos
    for _, f in readers:
        f.close()

# --- Teste e Validação ---
def create_test_csv(filename, num_rows=1000):
    """Cria um arquivo CSV de teste com dados aleatórios."""
    import random
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'value'])  # Cabeçalho
        
        for i in range(num_rows):
            writer.writerow([
                f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",  # ID único
                f"Name-{random.randint(1, 100)}",
                random.random() * 100
            ])

def test_external_sort():
    """Testa a ordenação externa com um arquivo pequeno."""
    input_file = 'test_input.csv'
    output_file = 'test_output.csv'
    
    # Cria arquivo de teste
    create_test_csv(input_file, num_rows=1000)
    
    # Executa a ordenação
    external_sort(
        input_file=input_file,
        output_file=output_file,
        key_column='id',  # Ou 0 para índice
        ascending=True,
        buffer_size=100  # Processa em chunks de 100 linhas
    )
    
    # Validação
    with open(output_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        prev_id = None
        
        for row in reader:
            current_id = row[0]
            if prev_id is not None:
                if current_id < prev_id:
                    print(f"ERRO: {current_id} vem depois de {prev_id}")
                    return False
            prev_id = current_id
    
    print("Teste passou! Arquivo está ordenado corretamente.")
    return True

# Executar o teste
if __name__ == '__main__':
    test_external_sort()