CREATE OR REPLACE FUNCTION public.clone_schema
(
	source_schema text,
	dest_schema text
)
RETURNS void AS
\$BODY$
DECLARE
empresa_id integer;
empresa_copy integer;
cadastro_id integer;
object text;
table_ text;
buffer text;
default_ text;
column_ text;
constraint_name_ text;
constraint_def_ text;
trigger_name_ text; 
trigger_timing_ text; 
trigger_events_ text; 
trigger_orientation_ text;
trigger_action_ text;
BEGIN
	-- selecionar empresa
	FOR empresa_copy IN
		SELECT id FROM public.empresas WHERE schemaname = source_schema limit 1
		LOOP END LOOP;
	-- create schema
	EXECUTE 'CREATE SCHEMA ' || dest_schema ;
	-- create sequences
	FOR object IN
		SELECT sequence_name::text FROM information_schema.SEQUENCES WHERE sequence_schema = source_schema
		LOOP
			IF object not in('regua_cobrancas_id_seq','templates_id_seq','arquivos_cad_debito_id_seq', 'timelines_id_seq', 'arquivos_id_seq', 'arquivocontents_id_seq', 'faturas_id_seq')  THEN
				EXECUTE 'CREATE SEQUENCE ' || dest_schema || '.' || object;
			END IF;
END LOOP;

-- Criar empresa
INSERT INTO public.empresas VALUES (nextval('public.empresas_id_seq'::regclass),'Empresa Modelo - ' || dest_schema, 'Empresa Modelo - ' || dest_schema, '00.000.000/0000-00', NULL, '79.800-004', 'DOURADOS', 'MS', 'CENTRO', 'AV MARCELINO PIRES', '1678', true, '2017-02-23 09:52:59-04', '2017-02-23 09:52:59-04', 3, '1º Andar', '(67) 2108-7007', 'Essencial', true, source_schema, false, dest_schema, false, false, NULL, 'America/Sao_Paulo');

-- Criar tabelas
FOR object IN
	SELECT table_name::text FROM information_schema.TABLES WHERE table_schema = source_schema
	LOOP
		buffer := dest_schema || '.' || object;
		-- Criar tabelas
		EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || source_schema || '.' || object || ' INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';
		-- corrigir padrões de sequência
		FOR column_, default_ IN
			SELECT column_name::text, REPLACE(column_default::text, source_schema||'.', dest_schema||'.') FROM information_schema.COLUMNS WHERE table_schema = dest_schema AND table_name = object AND column_default LIKE 'nextval(%' || source_schema || '.%::regclass)'
			LOOP
				IF object not in('regua_cobrancas','templates','cadastros','arquivos_cad_debito', 'timelines', 'arquivos', 'arquivocontents', 'faturas')  THEN
					EXECUTE 'ALTER TABLE ' || buffer || ' ALTER COLUMN ' || column_ || ' SET DEFAULT ' || default_;
				END IF;
	END LOOP;

-- Criar trigger
FOR trigger_name_, trigger_timing_, trigger_events_, trigger_orientation_, trigger_action_ IN
	SELECT trigger_name::text, action_timing::text, string_agg(event_manipulation::text, ' OR '), action_orientation::text, action_statement::text FROM information_schema.TRIGGERS WHERE event_object_schema=source_schema and event_object_table=object GROUP BY trigger_name, action_timing, action_orientation, action_statement
	LOOP
		EXECUTE 'CREATE TRIGGER ' || trigger_name_ || ' ' || trigger_timing_ || ' ' || trigger_events_ || ' ON ' || buffer || ' FOR EACH ' || trigger_orientation_ || ' ' || trigger_action_;
		EXECUTE 'ALTER TABLE ' || dest_schema || '.movcontas DISABLE TRIGGER movcontas_update';
	END LOOP;

--- Update na empresa_id
FOR object IN
	SELECT table_name::text FROM information_schema.columns WHERE table_schema = dest_schema and column_name = 'empresa_id'
	LOOP
		buffer := dest_schema || '.' || object;

		FOR empresa_id IN
			select id::integer from public.empresas where schemaname = dest_schema
		  LOOP
		  	EXECUTE 'update  ' || buffer || ' set empresa_id=' || empresa_id;
		  END LOOP;
	END LOOP;
END LOOP;

-- Popular banco	
FOR object IN
	SELECT table_name::text FROM information_schema.TABLES WHERE table_schema = source_schema
	LOOP
		buffer := dest_schema || '.' || object;

		-- Tabelas que não serão copiadas ou são casos epeciais
		IF object not in('regua_cobrancas','templates','cadastros','arquivos_cad_debito', 'timelines', 'arquivos', 'arquivocontents', 'movcontas','notas_fiscais', 'movcontas_rateios', 'faturas', 'movcontas_items')  THEN
			EXECUTE 'INSERT INTO ' || buffer || ' select * from ' || source_schema || '.' || object;
		END IF;

		-- Inserir movcontas(receitas e despesas)
		IF object = 'movcontas'  THEN	
			EXECUTE 'INSERT INTO ' || buffer || ' select * from ' || source_schema || '.' || object || ' where fatura_id is null order by id desc limit 1000';
		END IF;

		-- Realizar as inserções, apenas da movcontas que foram adicionada no sistema.
		IF object in('notas_fiscais', 'movcontas_rateios', 'movcontas_items')  THEN
			EXECUTE 'INSERT INTO ' || buffer || ' select * from ' || source_schema || '.' || object || ' where movconta_id in (select id from ' || dest_schema || '.movcontas) order by id desc limit 1000';
		END IF;

		-- Resetar configurações gateways_opts
		IF object = 'gateways_opts'  THEN	
			EXECUTE 'update ' || buffer || ' set valor = 0';
		END IF;

		IF object = 'cadastros' THEN
			EXECUTE 'INSERT INTO ' || buffer || ' select * from ' || source_schema || '.' || object;
			-- Criar registros em empresas_users
			EXECUTE 'update ' || dest_schema  || '.cadastros set user_id = null where email not like ''%@dourasoft.com.br%''';
			EXECUTE 'insert into public.empresas_users' || ' select user_id,' || empresa_id  || '::integer as empresa_id from ' || dest_schema  || '.cadastros' || ' where  user_id is not null';
			EXECUTE 'insert into public.usersextras (user_id, empresa_id, campo, valor)' || ' select user_id, ' || empresa_id || '::integer as empresa_id ' || ', campo, valor from public.usersextras where empresa_id = ' || empresa_copy;
			EXECUTE 'update ' || buffer || ' set empresa_id = ' || empresa_id;
		END IF;

		-- Relatorios
		IF object = 'relatorios'  THEN	
			EXECUTE 'update '  || dest_schema  || '.relatorios set sqltext = data.text from (select id as rela_id, replace(sqltext,' || '''' || source_schema || '.'''  || ',' || '''' || dest_schema || '.' || '''' ||') as text from ' || dest_schema  ||'.relatorios) as data where id = data.rela_id';
		END IF;
	END LOOP;

-- Setar sequencias com valor maximo
FOR table_ IN
	SELECT table_name::text FROM information_schema.TABLES WHERE table_schema = dest_schema
	LOOP
		FOR object IN
			SELECT sequence_name::text FROM information_schema.SEQUENCES WHERE sequence_schema = dest_schema and sequence_name like '%' || table_ || '%'
			LOOP
				EXECUTE 'SELECT setval(''' || dest_schema || '.' || object  || ''''|| ',(select max(id) from ' || dest_schema || '.' || table_ || ')::integer)';
			END LOOP;
	END LOOP;

-- criar chaves estrangeiras
FOR object IN
	SELECT table_name::text FROM information_schema.TABLES WHERE table_schema = source_schema
	LOOP
		buffer := dest_schema || '.' || object;
		FOR constraint_name_, constraint_def_ IN
			SELECT conname::text, REPLACE(pg_get_constraintdef(pg_constraint.oid), source_schema||'.', dest_schema||'.') FROM pg_constraint INNER JOIN pg_class ON conrelid=pg_class.oid INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace WHERE contype='f' and relname=object and nspname=source_schema
			LOOP
				IF object not in('regua_cobrancas','templates','cadastros','arquivos_cad_debito', 'timelines', 'arquivos', 'arquivocontents', 'faturas')  THEN
					EXECUTE 'ALTER TABLE '|| buffer ||' ADD CONSTRAINT '|| constraint_name_ ||' '|| constraint_def_;
				END IF;
			END LOOP;
	END LOOP;
END;
\$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

