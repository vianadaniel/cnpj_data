const { Pool } = require('pg');
const config = require('../config');

// Pool de conexão com o PostgreSQL
const pool = new Pool({
  user: config.dbUser,
  host: config.dbHost,
  database: config.dbName,
  password: config.dbPassword,
  port: config.dbPort,
});

// Função para criar esquema do banco de dados
async function createDatabase() {
  const client = await pool.connect();

  try {
    console.log('Criando esquema do banco de dados...');

    // Inicia uma transação
    await client.query('BEGIN');

    // Cria tabelas
    await client.query(`
      -- Tabela de empresas
      CREATE TABLE IF NOT EXISTS empresas (
        cnpj_basico TEXT PRIMARY KEY,
        razao_social TEXT,
        natureza_juridica TEXT,
        qualificacao_responsavel TEXT,
        capital_social NUMERIC,
        porte_empresa TEXT,
        ente_federativo TEXT
      );

      -- Tabela de estabelecimentos
      CREATE TABLE IF NOT EXISTS estabelecimentos (
        cnpj_basico TEXT,
        cnpj_ordem TEXT,
        cnpj_dv TEXT,
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
    `);

    // Adicionar coluna virtual como uma coluna gerada no PostgreSQL
    await client.query(`
      ALTER TABLE estabelecimentos
      ADD COLUMN IF NOT EXISTS cnpj_completo TEXT GENERATED ALWAYS AS (cnpj_basico || cnpj_ordem || cnpj_dv) STORED;
    `);

    // Criar índices para melhorar performance
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_cnpj_completo ON estabelecimentos(cnpj_completo);
      CREATE INDEX IF NOT EXISTS idx_razao_social ON empresas(razao_social);
    `);

    // Criar tabela CNAE
    await client.query(`
      CREATE TABLE IF NOT EXISTS cnae (
          codigo TEXT PRIMARY KEY,
          descricao TEXT
      );
    `);

    // Criar tabela Motivos
    await client.query(`
      CREATE TABLE IF NOT EXISTS motivos (
          codigo TEXT PRIMARY KEY,
          descricao TEXT
      );
    `);

    // Criar tabela Municípios
    await client.query(`
      CREATE TABLE IF NOT EXISTS municipios (
          codigo TEXT PRIMARY KEY,
          nome TEXT NOT NULL,
          uf TEXT NOT NULL
      );
    `);

    // Criar tabela Naturezas Jurídicas
    await client.query(`
      CREATE TABLE IF NOT EXISTS naturezas_juridicas (
          codigo TEXT PRIMARY KEY,
          descricao TEXT NOT NULL
      );
    `);

    // Criar tabela Países
    await client.query(`
      CREATE TABLE IF NOT EXISTS paises (
          codigo TEXT PRIMARY KEY,
          nome TEXT NOT NULL
      );
    `);

    // Criar tabela Qualificações
    await client.query(`
      CREATE TABLE IF NOT EXISTS qualificacoes (
          codigo TEXT PRIMARY KEY,
          descricao TEXT NOT NULL
      );
    `);

    // Criar tabela Simples Nacional
    await client.query(`
      CREATE TABLE IF NOT EXISTS simples (
          cnpj_basico TEXT PRIMARY KEY,
          opcao_simples INTEGER NOT NULL,
          data_opcao_simples TEXT,
          data_exclusao_simples TEXT,
          opcao_mei INTEGER NOT NULL,
          data_opcao_mei TEXT,
          data_exclusao_mei TEXT
      );
    `);

    // Criar tabela Sócios
    await client.query(`
      CREATE TABLE IF NOT EXISTS socios (
          id SERIAL PRIMARY KEY,
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
      );
    `);

    // Commit da transação
    await client.query('COMMIT');
    console.log('Esquema do banco de dados criado com sucesso');

    return pool;
  } catch (error) {
    // Rollback em caso de erro
    await client.query('ROLLBACK');
    console.error('Erro ao criar esquema do banco de dados:', error);
    throw error;
  } finally {
    client.release();
  }
}

// Função para criar índices adicionais
async function createAdditionalIndices() {
  const client = await pool.connect();

  try {
    console.log('Criando índices adicionais...');

    // Inicia uma transação
    await client.query('BEGIN');

    // Índices para estabelecimentos
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_situacao_cadastral ON estabelecimentos(situacao_cadastral);
      CREATE INDEX IF NOT EXISTS idx_uf ON estabelecimentos(uf);
      CREATE INDEX IF NOT EXISTS idx_municipio ON estabelecimentos(municipio);
      CREATE INDEX IF NOT EXISTS idx_cnae_principal ON estabelecimentos(cnae_principal);
    `);

    // Índice para busca por descrição de CNAE
    await client.query('CREATE INDEX IF NOT EXISTS idx_cnae_descricao ON cnae(descricao);');

    // Índice para busca por descrição de Motivos
    await client.query('CREATE INDEX IF NOT EXISTS idx_motivos_descricao ON motivos(descricao);');

    // Índices para busca por nome e UF de Municípios
    await client.query('CREATE INDEX IF NOT EXISTS idx_municipios_nome ON municipios(nome);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_municipios_uf ON municipios(uf);');

    // Índice para busca por descrição de Naturezas Jurídicas
    await client.query('CREATE INDEX IF NOT EXISTS idx_naturezas_juridicas_descricao ON naturezas_juridicas(descricao);');

    // Índice para busca por nome de Países
    await client.query('CREATE INDEX IF NOT EXISTS idx_paises_nome ON paises(nome);');

    // Índice para busca por descrição de Qualificações
    await client.query('CREATE INDEX IF NOT EXISTS idx_qualificacoes_descricao ON qualificacoes(descricao);');

    // Índices para busca por opção pelo Simples e MEI
    await client.query('CREATE INDEX IF NOT EXISTS idx_simples_opcao_simples ON simples(opcao_simples);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_simples_opcao_mei ON simples(opcao_mei);');

    // Índices para busca por sócios
    await client.query('CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios(cnpj_basico);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_socios_nome_socio ON socios(nome_socio);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_socios_qualificacao_socio ON socios(qualificacao_socio);');

    // Commit da transação
    await client.query('COMMIT');
    console.log('Índices adicionais criados com sucesso');
  } catch (error) {
    // Rollback em caso de erro
    await client.query('ROLLBACK');
    console.error('Erro ao criar índices adicionais:', error);
    throw error;
  } finally {
    client.release();
  }
}

// Função para fechar a conexão com o banco de dados
async function closeConnection() {
  await pool.end();
  console.log('Conexão com o banco de dados fechada');
}

module.exports = {
  createDatabase,
  createAdditionalIndices,
  closeConnection,
  pool
};