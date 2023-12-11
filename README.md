# First-Hadoop-Project

Este projeto implementa uma aplicação MapReduce para realizar análises sobre um conjunto de dados. O código está escrito em Java e utiliza o framework Apache Hadoop para processamento distribuído. O objetivo é extrair informações úteis e estatísticas a partir de transações registradas.

## Tarefas Realizadas

### Tarefa 1: Contar Transações Relacionadas ao Brasil

Esta tarefa conta o número de transações relacionadas ao Brasil. Os resultados são salvos no diretório `output1/resultados.txt`.

### Tarefa 2: Contar Transações por Ano

Aqui, o código conta o número de transações por ano. Os resultados são armazenados no diretório `output2/resultados.txt`.

### Tarefa 3: Contar Transações por Tipo e Ano

Nesta tarefa, o programa conta o número de transações por tipo e ano. Os resultados são salvos no diretório `output3/resultados.txt`.

### Tarefa 4: Calcular a Média de Valores por Ano

A aplicação calcula a média de valores por ano nas transações. Os resultados são armazenados no diretório `output4/resultados.txt`.

### Tarefa 5: Calcular a Média de Preços por Unidade, Ano e Categoria

Nesta tarefa, a média de preços é calculada com base na unidade, ano e categoria das transações. Os resultados são salvos no diretório `output5/resultados.txt`.

### Tarefa 6: Calcular Valores Máximo, Mínimo e Médio de Preços

Por fim, o código calcula os valores máximo, mínimo e médio de preços em determinadas condições. Os resultados são armazenados no diretório `output6/resultados.txt`.

## Execução

Certifique-se de ter o ambiente Hadoop configurado corretamente. Execute o seguinte comando para iniciar o processamento:

```bash
hadoop jar NOME_DO_JAR.jar exercicios.MapReduce input output
