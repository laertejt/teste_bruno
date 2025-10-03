from fastapi import FastAPI 
from typing import List
import pandas as pd 
from pathlib import Path
from sqlalchemy import create_engine, text 

from dotenv import load_dotenv
import os
load_dotenv()  # take environment variables
senha_db=os.getenv("senha_db")



engine = create_engine(f"mysql+pymysql://root:{senha_db}@127.0.0.1:3333/db_escola")


app = FastAPI()


@app.get("/matricula-por-id/", response_model=List[dict])
def consulta_alunos(id: int):
    df_endereco = pd.read_sql(f"""select * from tb_alunos 
                               where id = {id}""",con=engine)

    return df_endereco.to_dict(orient="records")


@app.post("/inserir-aluno/", response_model=dict)
def inserir_aluno(dados_alunos: dict):
    with engine.begin() as conn:
        conn.execute(
            text("""
                Insert into tb_alunos
                 ( matricula, nome, email, endereco_id)
                 values( :matricula, :nome, :email, :endereco_id)
                 ;
                """), dados_alunos
        )

    return {"mensagem": "aluno cadastrado com sucesso"}


@app.delete("/deletar-aluno-id/", response_model=dict)
def deletar_aluno(id: int):
    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM tb_alunos
                WHERE id = :id
            """),
            {"id": id}
        )
    return {"mensagem": f"Aluno com id {id} deletado com sucesso"}


@app.put("/atualizar-aluno/", response_model=dict)
def atualizar_aluno(dados_aluno: dict):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE tb_alunos
                SET matricula = :matricula,
                    nome = :nome,
                    email = :email,
                    endereco_id = :endereco_id
                WHERE id = :id
            """),
            dados_aluno
        )
    return {"mensagem": f"Aluno com id {id} atualizado com sucesso"}



@app.get("/enderecos-por-id/", response_model=List[dict])
def consulta_endereco(id: int):
    df_enderecos = pd.read_sql(f"""select * from tb_enderecos 
                               where id = {id}""",con=engine)

    return df_enderecos.to_dict(orient="records")



@app.get("/enderecos-por-estado/", response_model=List[dict])
def consulta_endereco_estado(estado: str):
    df_enderecos = pd.read_sql(
        f"SELECT * FROM tb_enderecos WHERE estado = '{estado}'",
        con=engine
    )
    return df_enderecos.to_dict(orient="records")




@app.post("/inserir-endereco/", response_model=dict)
def inserir_endereco(dados_endereco: dict):
    with engine.begin() as conn:
        conn.execute(
            text("""
                Insert into tb_enderecos
                 (cep, endereco, bairro, cidade, estado, regiao)
                 values(:cep, :endereco, :bairro, :cidade, :estado, :regiao)
                 ;
                """), dados_endereco
        )

    return {"mensagem": "endereco cadastrado com sucesso"}

@app.put("/atualizar-endereco/", response_model=dict)
def atualizar_endereco(dados_endereco: dict):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE tb_enderecos
                SET cep = :cep,
                    endereco = :endereco,
                    bairro = :bairro,
                    cidade = :cidade,
                    estado = :estado,
                    regiao = :regiao
                WHERE id = :id
            """),
            dados_endereco
        )
    return {"mensagem": "endereco atualizado com sucesso"}



@app.delete("/deletar-endereco-por-id/", response_model=dict)
def deletar_endereco(id: int):
    with engine.begin() as conn:
        conn.execute(
            text("delete from tb_enderecos where id = :id"),
            {"id": id}
        )
    return {"mensagem": f"Endereço com id {id} deletado com sucesso"}
