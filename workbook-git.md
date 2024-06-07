
biblioteca no git 

git init
-- Descrição: Inicializa um novo repositório Git local. Este comando cria um novo subdiretório chamado .git que contém todos os arquivos necessários para o repositório.

git log
-- acessando o log do arquivo

git checkout arquivo.name
-- Comando para voltar a versao anterior do arquivo, voltando o arquivo para o ultimo status da branch main 

.gitignore
-- ignorar arquivos para nao subir no git, deve se criar um arquivo .gitignore e dentro do arquivo deve ser descrito os arquivos que nao devem serem enviados para o git

git reset --hard origin/main 
--forçando o codigo para retornar o ultimo commit presente na main

git clone [url]
-- Descrição: Faz uma cópia de um repositório Git existente. A URL pode ser o endereço de um repositório remoto.

git add [arquivo]
Descrição: Adiciona um arquivo à área de staging (índice), preparando-o para ser incluído no próximo commit.

git commit -m "mensagem"
Descrição: Salva as alterações que estão na área de staging no repositório local, juntamente com uma mensagem de commit descrevendo o que foi alterado.

git status
Descrição: Mostra o estado atual do repositório, incluindo mudanças que foram staged, mudanças que não foram staged, e arquivos que não estão sendo rastreados pelo 
Git.

git push [remoto] [branch]
Descrição: Envia as alterações de uma branch local para uma branch remota. Por exemplo, enviar mudanças da branch local main para a branch main no repositório remoto.

git pull [remoto] [branch]
Descrição: Atualiza a branch local com as últimas mudanças da branch correspondente no repositório remoto, combinando as mudanças remotas com as locais.

git branch [nome-da-branch]
Descrição: Cria uma nova branch a partir da branch atual.

git branch -b [nome-da-branch]
criar a branch e ir direto para ela

git branch -d [nome-da-branch]
deletar a branch

git checkout [nome-da-branch ou commit]
Descrição: Alterna entre branches ou restaura versões de arquivos no diretório de trabalho.

git merge [nome-da-branch]
Descrição: Combina a história da branch especificada com a branch atual. Usado para incorporar mudanças de uma branch para outra.

git log
Descrição: Mostra o histórico de commits na branch atual, listando os últimos commits primeiro.

git fetch [remoto]
Descrição: Baixa todas as mudanças de um repositório remoto, mas não atualiza as branches locais. Usado para ver o que outros estão fazendo, sem mesclar essas mudanças no seu trabalho local.

git diff
Descrição: Mostra as diferenças entre arquivos ou commits. Pode ser usado para ver mudanças que não foram staged ou mudanças entre branches.

git reset
Descrição: Redefine a área de staging (mas não o diretório de trabalho) para o estado de um commit especificado, desfazendo preparações (staging) e opcionalmente alterando a história do repositório.

git rm [arquivo]
Descrição: Remove arquivos do diretório de trabalho e da área de staging.

git config --global user.name usuario
git config --global user.email usuario@gmail.com

git stash 
retorna ao status atual da branch

git stash apply [numero do retorno do stash]
escolha qual versao do branch voce quer voltar 

git stash list 
mostra todas as stash criadas 

git stash show -p [id]
mostra as alteracoes que aconteceram com a branch 0 

git gc
codigo para dar mais performace para o repositorio, não é possivel ver as alterações realizados pelo comando 

gitshortlog
comando para verificar os commits realizados 

git diff HEAD: nome.arquivo
comparar dois arquivos, o do git e o que está na sua maquina, mostrar essa diferença

git reflog
exibe os logs de no maximo durante 30 dias 



