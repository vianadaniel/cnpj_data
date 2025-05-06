const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const config = require('../config');

// Função para criar esquema do banco de dados
async function createDatabase() {
  // Abre o banco de dados
  const db = await open({
    filename: config.dbPath,
    driver: sqlite3.Database
  });

  console.log('Criando esquema do banco de dados...');

  // Cria tabelas
  await db.exec(`
    -- Tabela de empresas
    CREATE TABLE IF NOT EXISTS empresas (
      cnpj_basico TEXT PRIMARY KEY,
      razao_social TEXT,
      natureza_juridica TEXT,
      qualificacao_responsavel TEXT,
      capital_social REAL,
      porte_empresa TEXT,
      ente_federativo TEXT
    );

    -- Tabela de estabelecimentos
    CREATE TABLE IF NOT EXISTS estabelecimentos (
      cnpj_basico TEXT,
      cnpj_ordem TEXT,
      cnpj_dv TEXT,
      cnpj_completo TEXT GENERATED ALWAYS AS (cnpj_basico || cnpj_ordem || cnpj_dv) VIRTUAL,
      nome_fantasia TEXT,
      situacao_cadastral TEXT,
      data_situacao_cadastral TEXT,
      motivo_situacao_cadastral TEXT,
      cidade_exterior TEXT,
      pais TEXT,
      data_inicio_atividade TEXT,
      cnae_principal TEXT,
      cnaes_secundarios TEXT,
      tipo_logradouro TEXT,
      logradouro TEXT,
      numero TEXT,
      complemento TEXT,
      bairro TEXT,
      cep TEXT,
      uf TEXT,
      municipio TEXT,
      ddd1 TEXT,
      telefone1 TEXT,
      ddd2 TEXT,
      telefone2 TEXT,
      email TEXT,
      PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv),
      FOREIGN KEY (cnpj_basico) REFERENCES empresas(cnpj_basico)
    );

    -- Índices para melhorar performance
    CREATE INDEX IF NOT EXISTS idx_cnpj_completo ON estabelecimentos(cnpj_completo);
    CREATE INDEX IF NOT EXISTS idx_razao_social ON empresas(razao_social);
  `);

  // Criar tabela CNAE
  await db.run(`
    CREATE TABLE IF NOT EXISTS cnae (
        codigo TEXT PRIMARY KEY,
        descricao TEXT
    )
`);

  // Criar tabela Motivos
  await db.run(`
    CREATE TABLE IF NOT EXISTS motivos (
        codigo TEXT PRIMARY KEY,
        descricao TEXT
    )
`);

  // Criar tabela Municípios
  await db.run(`
    CREATE TABLE IF NOT EXISTS municipios (
        codigo TEXT PRIMARY KEY,
        nome TEXT NOT NULL,
        uf TEXT NOT NULL
    )
`);

  // Criar tabela Naturezas Jurídicas
  await db.run(`
    CREATE TABLE IF NOT EXISTS naturezas_juridicas (
        codigo TEXT PRIMARY KEY,
        descricao TEXT NOT NULL
    )
`);

  // Criar tabela Países
  await db.run(`
    CREATE TABLE IF NOT EXISTS paises (
        codigo TEXT PRIMARY KEY,
        nome TEXT NOT NULL
    )
`);

  // Criar tabela Qualificações
  await db.run(`
    CREATE TABLE IF NOT EXISTS qualificacoes (
        codigo TEXT PRIMARY KEY,
        descricao TEXT NOT NULL
    )
`);

  // Criar tabela Simples Nacional
  await db.run(`
    CREATE TABLE IF NOT EXISTS simples (
        cnpj_basico TEXT PRIMARY KEY,
        opcao_simples INTEGER NOT NULL,
        data_opcao_simples TEXT,
        data_exclusao_simples TEXT,
        opcao_mei INTEGER NOT NULL,
        data_opcao_mei TEXT,
        data_exclusao_mei TEXT
    )
`);

  // Criar tabela Sócios
  await db.run(`
    CREATE TABLE IF NOT EXISTS socios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cnpj_basico TEXT NOT NULL,
        identificador_socio TEXT NOT NULL,
        nome_socio TEXT,
        cnpj_cpf_socio TEXT,
        qualificacao_socio TEXT,
        data_entrada_sociedade TEXT,
        pais TEXT,
        representante_legal TEXT,
        nome_representante TEXT,
        qualificacao_representante TEXT,
        faixa_etaria TEXT
    )
`);

  console.log('Esquema do banco de dados criado com sucesso');
  return db;
}

// Função para criar índices adicionais
async function createAdditionalIndices(db) {
  console.log('Criando índices adicionais...');
  await db.exec(`
        CREATE INDEX IF NOT EXISTS idx_situacao_cadastral ON estabelecimentos(situacao_cadastral);
        CREATE INDEX IF NOT EXISTS idx_uf ON estabelecimentos(uf);
        CREATE INDEX IF NOT EXISTS idx_municipio ON estabelecimentos(municipio);
        CREATE INDEX IF NOT EXISTS idx_cnae_principal ON estabelecimentos(cnae_principal);
    `);

  // Índice para busca por descrição de CNAE
  await db.run('CREATE INDEX IF NOT EXISTS idx_cnae_descricao ON cnae(descricao)');

  // Índice para busca por descrição de Motivos
  await db.run('CREATE INDEX IF NOT EXISTS idx_motivos_descricao ON motivos(descricao)');

  // Índices para busca por nome e UF de Municípios
  await db.run('CREATE INDEX IF NOT EXISTS idx_municipios_nome ON municipios(nome)');
  await db.run('CREATE INDEX IF NOT EXISTS idx_municipios_uf ON municipios(uf)');

  // Índice para busca por descrição de Naturezas Jurídicas
  await db.run('CREATE INDEX IF NOT EXISTS idx_naturezas_juridicas_descricao ON naturezas_juridicas(descricao)');

  // Índice para busca por nome de Países
  await db.run('CREATE INDEX IF NOT EXISTS idx_paises_nome ON paises(nome)');

  // Índice para busca por descrição de Qualificações
  await db.run('CREATE INDEX IF NOT EXISTS idx_qualificacoes_descricao ON qualificacoes(descricao)');

  // Índices para busca por opção pelo Simples e MEI
  await db.run('CREATE INDEX IF NOT EXISTS idx_simples_opcao_simples ON simples(opcao_simples)');
  await db.run('CREATE INDEX IF NOT EXISTS idx_simples_opcao_mei ON simples(opcao_mei)');

  // Índices para busca por sócios
  await db.run('CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios(cnpj_basico)');
  await db.run('CREATE INDEX IF NOT EXISTS idx_socios_nome_socio ON socios(nome_socio)');
  await db.run('CREATE INDEX IF NOT EXISTS idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio)');
  await db.run('CREATE INDEX IF NOT EXISTS idx_socios_qualificacao_socio ON socios(qualificacao_socio)');

  console.log('Índices adicionais criados com sucesso');
}

module.exports = {
  createDatabase,
  createAdditionalIndices
};