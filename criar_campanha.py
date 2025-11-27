#!/usr/bin/env python3
"""
Script para criar e enviar campanhas do Mailchimp com imóveis semelhantes
"""

import os
import sys
import logging
import hashlib
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import radians, cos, sin, acos
import threading
import json
import unicodedata
import mysql.connector
from mysql.connector import Error
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
   
        logging.FileHandler('mailchimp_campanha.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def normalize_bairro(nome: str) -> str:
    """Normaliza nome de bairro removendo acentos e caracteres não alfanuméricos."""
    if not nome:
        return None
    normalized = unicodedata.normalize('NFKD', nome)
    normalized = ''.join(ch for ch in normalized if ch.isalnum())
    return normalized.lower()


BAIRRO_RAIO_MAP = {
    "auxiliadora": [
        "Auxiliadora", "Mont Serrat", "Bela Vista", "Moinhos de Vento",
        "Independência", "Rio Branco", "Boa Vista"
    ],
    "belavista": [
        "Bela Vista", "Mont Serrat", "Petrópolis", "Auxiliadora",
        "Boa Vista", "Moinhos de Vento", "Chácara das Pedras"
    ],
    "boavista": [
        "Boa Vista", "Bela Vista", "Mont Serrat", "Chácara das Pedras",
        "Três Figueiras", "Petrópolis", "Passo da Areia"
    ],
    "bomfim": [
        "Bom Fim", "Independência", "Rio Branco",
        "Moinhos de Vento", "Higienópolis", "Jardim Botânico"
    ],
    "centralparque": [
        "Central Parque", "Jardim do Salso", "Jardim Botânico",
        "Petrópolis", "Partenon", "Agronomia"
    ],
    "centralpark": [
        "Central Parque", "Jardim do Salso", "Jardim Botânico",
        "Petrópolis", "Partenon", "Agronomia"
    ],
    "chacaradaspedras": [
        "Chácara das Pedras", "Três Figueiras", "Boa Vista",
        "Bela Vista", "Petrópolis", "Higienópolis"
    ],
    "passodareia": [
        "Passo da Areia", "Boa Vista", "Chácara das Pedras",
        "Cristo Redentor", "Jardim Europa"
    ],
    "tresfigueiras": [
        "Três Figueiras", "Boa Vista", "Chácara das Pedras",
        "Bela Vista", "Petrópolis", "Mont Serrat"
    ],
    "meninodeus": [
        "Menino Deus", "Praia de Belas", "Santana",
        "Azenha", "Centro Histórico", "Cidade Baixa"
    ],
    "higienopolis": [
        "Higienópolis", "Petrópolis", "Moinhos de Vento",
        "Bom Fim", "Jardim Botânico", "Santana"
    ],
    "independencia": [
        "Independência", "Bom Fim", "Centro Histórico",
        "Moinhos de Vento", "Floresta", "Rio Branco"
    ],
    "jardimbotanico": [
        "Jardim Botânico", "Petrópolis", "Jardim Europa",
        "Jardim do Salso", "Partenon", "Central Parque"
    ],
    "jardimeuropa": [
        "Jardim Europa", "Chácara das Pedras", "Boa Vista",
        "Três Figueiras", "Passo da Areia", "Petrópolis"
    ],
    "moinhosdevento": [
        "Moinhos de Vento", "Bela Vista", "Mont Serrat",
        "Independência", "Auxiliadora", "Rio Branco"
    ],
    "montserrat": [
        "Mont Serrat", "Bela Vista", "Auxiliadora",
        "Moinhos de Vento", "Boa Vista", "Petrópolis"
    ],
    "petropolis": [
        "Petrópolis", "Bela Vista", "Mont Serrat",
        "Boa Vista", "Jardim Botânico", "Três Figueiras", "Chácara das Pedras"
    ],
    "riobranco": [
        "Rio Branco", "Bom Fim", "Independência",
        "Moinhos de Vento", "Mont Serrat", "Petrópolis"
    ],
    "ipanema": [
        "Ipanema", "Cavalhada", "Pedra Redonda",
        "Tristeza", "Vila Assunção", "Jardim Isabel"
    ],
}


class MailchimpCampanha:
    """Classe para criar campanhas no Mailchimp"""

    def __init__(self):
        """Inicializa conexões"""
        # Configuração banco de Leads
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': int(os.getenv('DB_PORT', 3306))
        }

        # Configuração banco de Imóveis
        self.db_imoveis_config = {
            'host': os.getenv('DB_IMOVEIS_HOST'),
            'database': os.getenv('DB_IMOVEIS_NAME'),
            'user': os.getenv('DB_IMOVEIS_USER'),
            'password': os.getenv('DB_IMOVEIS_PASSWORD'),
            'port': int(os.getenv('DB_IMOVEIS_PORT', 3306))
        }

        # Configuração Mailchimp
        self.mailchimp_client = MailchimpMarketing.Client()
        self.mailchimp_client.set_config({
            "api_key": os.getenv('MAILCHIMP_API_KEY'),
            "server": os.getenv('MAILCHIMP_SERVER_PREFIX')
        })

        self.list_id = os.getenv('MAILCHIMP_LIST_ID')

        # Caches e controle de concorrência
        self._imoveis_by_codigo: Dict[str, Dict] = {}
        self._imoveis_list: List[Dict] = []
        self._coords_by_codigo: Dict[str, tuple] = {}
        self._semelhantes_cache: Dict[str, List[Dict]] = {}
        self._cache_lock = threading.Lock()
        self.max_workers = int(os.getenv('WORKERS', '6'))

    def load_all_imoveis(self):
        """Carrega todos os imóveis em memória (uma vez)."""
        if self._imoveis_list:
            return
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_imoveis_config)
            cursor = connection.cursor(dictionary=True)
            query = (
                """
                SELECT Codigo, Dormitorios, AreaPrivativa, ValorVenda,
                       Foto, TituloSite, Endereco, BairroComercial
                FROM tb_imoveis
                """
            )
            cursor.execute(query)
            rows = cursor.fetchall() or []
            with self._cache_lock:
                self._imoveis_list = rows
                self._imoveis_by_codigo = {str(r['Codigo']): r for r in rows}
            logger.info(f"Imóveis carregados em cache: {len(rows)}")
        except Error as e:
            logger.error(f"Erro ao carregar imóveis: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def load_all_coordenadas(self):
        """Carrega todas as coordenadas de agenciamentos (banco de Leads)."""
        if self._coords_by_codigo:
            return
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)
            query = (
                """
                SELECT codigo_imovel, latitude, longitude
                FROM agenciamentos
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                """
            )
            cursor.execute(query)
            rows = cursor.fetchall() or []
            with self._cache_lock:
                self._coords_by_codigo = {
                    str(r['codigo_imovel']): (r['latitude'], r['longitude'])
                    for r in rows
                    if r.get('latitude') is not None and r.get('longitude') is not None
                }
            logger.info(f"Coordenadas carregadas em cache: {len(self._coords_by_codigo)}")
        except Error as e:
            logger.error(f"Erro ao carregar coordenadas: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_leads_from_database(self) -> List[Dict]:
        """Busca leads do banco de dados"""
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)

            query = """
            SELECT *,
              REGEXP_REPLACE(mkt_produto, '[^0-9]', '') AS mkt_produto_formatado
            FROM Leads
            WHERE email IS NOT NULL
              AND email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
              AND (
                tipo = 'MOR' OR fonte IN ('SITE', 'ZAP_IMOVEIS')
              )
              AND mkt_produto REGEXP '[0-9]'
            """

            cursor.execute(query)
            leads = cursor.fetchall()

            logger.info(f"Encontrados {len(leads)} leads no banco")
            return leads

        except Error as e:
            logger.error(f"Erro ao conectar ao banco: {e}")
            return []

        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_imovel_data(self, codigo: str) -> Dict:
        """Busca dados do imóvel a partir do cache (carregado uma vez)."""
        if not self._imoveis_by_codigo:
            self.load_all_imoveis()
        return self._imoveis_by_codigo.get(str(codigo))

    def get_coordenadas_imovel(self, codigo: str) -> tuple:
        """Obtém coordenadas a partir do cache."""
        if not self._coords_by_codigo:
            self.load_all_coordenadas()
        return self._coords_by_codigo.get(str(codigo), (None, None))

    def get_imoveis_semelhantes(self, imovel: Dict) -> List[Dict]:
        """Busca imóveis semelhantes a partir dos caches (sem JOIN entre bancos)."""
        codigo_key = str(imovel['Codigo'])
        # Cache por código de origem
        if codigo_key in self._semelhantes_cache:
            return self._semelhantes_cache[codigo_key]

        if not self._imoveis_list:
            self.load_all_imoveis()
        if not self._coords_by_codigo:
            self.load_all_coordenadas()

        try:
            lat_origem, lon_origem = self.get_coordenadas_imovel(codigo_key)
            alvo_valor = imovel.get('ValorVenda')

            if lat_origem and lon_origem:
                logger.info(f"Buscando imóveis semelhantes por localização (5km) para {codigo_key}")

                lat_r = radians(lat_origem)
                lon_r = radians(lon_origem)

                semelhantes: List[Dict] = []
                nivel: List[Dict] = []
                for cand in self._imoveis_list:
                    if str(cand['Codigo']) == codigo_key:
                        continue
                    if cand.get('Foto') is None or cand.get('TituloSite') is None:
                        continue
                    valor_c = cand.get('ValorVenda')
                    if valor_c is None or alvo_valor is None:
                        continue
                    if not (alvo_valor * 0.65 <= valor_c <= alvo_valor * 1.35):
                        continue
                    coord = self._coords_by_codigo.get(str(cand['Codigo']))
                    if not coord:
                        continue
                    lat_c, lon_c = coord
                    # Distância via fórmula do cosseno esférico (como no SQL)
                    d = 6371 * acos(
                        cos(lat_r) * cos(radians(lat_c)) * cos(radians(lon_c) - lon_r) +
                        sin(lat_r) * sin(radians(lat_c))
                    )
                    if d <= 5:
                        item = dict(cand)
                        item['distancia'] = d
                        nivel.append(item)

                nivel.sort(key=lambda x: x.get('distancia', 9999))
                semelhantes = nivel[:4]
            else:
                logger.warning(f"Imóvel {codigo_key} sem coordenadas, usando BairroComercial como fallback")
                bairro = imovel.get('BairroComercial')
                bairro_norm = normalize_bairro(bairro)
                raio_recomendado = BAIRRO_RAIO_MAP.get(bairro_norm)
                allowed_norms = set()
                if bairro_norm:
                    allowed_norms.add(bairro_norm)
                if raio_recomendado:
                    allowed_norms.update(
                        normalize_bairro(b) for b in raio_recomendado if normalize_bairro(b)
                    )
                semelhantes: List[Dict] = []
                for cand in self._imoveis_list:
                    if str(cand['Codigo']) == codigo_key:
                        continue
                    if cand.get('Foto') is None or cand.get('TituloSite') is None:
                        continue
                    valor_c = cand.get('ValorVenda')
                    if valor_c is None or alvo_valor is None:
                        continue
                    if not (alvo_valor * 0.65 <= valor_c <= alvo_valor * 1.35):
                        continue
                    cand_bairro_norm = normalize_bairro(cand.get('BairroComercial'))
                    if allowed_norms:
                        if not cand_bairro_norm or cand_bairro_norm not in allowed_norms:
                            continue
                    else:
                        if cand_bairro_norm != bairro_norm:
                            continue
                    semelhantes.append(cand)
                    if len(semelhantes) >= 4:
                        break

            with self._cache_lock:
                self._semelhantes_cache[codigo_key] = semelhantes
            return semelhantes
        except Exception as e:
            logger.error(f"Erro ao buscar imóveis semelhantes: {e}")
            return []

    def get_tag_segment_id(self, tag_campanha: str) -> int:
        """Busca o ID do segmento que o Mailchimp criou automaticamente para a tag"""
        try:
            # Aguarda um pouco para o Mailchimp processar as tags
            import time
            time.sleep(2)

            logger.info(f"Buscando segmento para tag: {tag_campanha}")
            segments = self.mailchimp_client.lists.list_segments(self.list_id, count=1000)

            for segment in segments.get('segments', []):
                if segment.get('name') == tag_campanha and segment.get('type') == 'static':
                    logger.info(f"Segmento encontrado: {tag_campanha} (ID: {segment['id']}, membros: {segment.get('member_count', 0)})")
                    return segment['id']

            logger.error(f"Segmento não encontrado para tag: {tag_campanha}")
            return None

        except ApiClientError as e:
            logger.error(f"Erro ao buscar segmento: {e.text}")
            return None

    def criar_campanha(self, emails_processados: List[str], tag_campanha: str, enviar_automaticamente: bool = True):
        """Cria campanha no Mailchimp usando o template HTML externo"""
        try:
            campaign_name = f"Imóveis Semelhantes - {datetime.now().strftime('%d/%m/%Y %H:%M')}"

            logger.info(f"Criando campanha para {len(emails_processados)} contatos com tag {tag_campanha}")

            # Busca o segmento que o Mailchimp criou automaticamente para a tag
            segment_id = self.get_tag_segment_id(tag_campanha)

            if not segment_id:
                logger.error(f"Não foi possível encontrar o segmento da tag {tag_campanha}")
                return None

            # Cria a campanha usando o segmento
            campaign = self.mailchimp_client.campaigns.create({
                "type": "regular",
                "recipients": {
                    "list_id": self.list_id,
                    "segment_opts": {
                        "saved_segment_id": int(segment_id)
                    }
                },
                "settings": {
                    "subject_line": "Seu novo lar está aqui!",
                    "from_name": "Urban Select",
                    "reply_to": "mkt@urban.imb.br",
                    "title": campaign_name
                }
            })

            campaign_id = campaign['id']
            logger.info(f"Campanha criada: {campaign_id} usando segmento estático {segment_id}")

            # Lê o template HTML de arquivo externo
            template_path = os.path.join(os.path.dirname(__file__), 'email_template.html')
            try:
                with open(template_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                logger.info(f"Template HTML carregado de: {template_path}")
            except FileNotFoundError:
                logger.error(f"Arquivo de template não encontrado: {template_path}")
                return None

            # Define o conteúdo HTML
            self.mailchimp_client.campaigns.set_content(campaign_id, {
                "html": html_content
            })

            logger.info(f"Conteúdo HTML adicionado à campanha {campaign_id}")

            # Envia automaticamente se solicitado
            if enviar_automaticamente:
                logger.info(f"🚀 Enviando campanha {campaign_id} automaticamente...")
                try:
                    self.mailchimp_client.campaigns.send(campaign_id)
                    logger.info(f"✅ Campanha {campaign_id} ENVIADA com sucesso para {len(emails_processados)} contato(s)!")
                except ApiClientError as send_error:
                    logger.error(f"❌ Erro ao enviar campanha: {send_error.text}")
                    logger.info(f"💡 Campanha {campaign_id} foi criada mas não foi enviada. Envie manualmente no Mailchimp.")
                    return campaign_id
            else:
                # Deixa como rascunho
                logger.info(f"✅ Campanha {campaign_id} criada como RASCUNHO")
                logger.info("⚠️  Acesse o Mailchimp para revisar e enviar manualmente")

            return campaign_id

        except ApiClientError as e:
            logger.error(f"Erro ao criar campanha: {e.text}")
            return None

    def formatar_valor_br(self, valor: float) -> str:
        """Formata valor para padrão brasileiro (ex: 470.000,00)"""
        if valor is None:
            return "0,00"

        # Formata com 2 casas decimais
        valor_str = f"{valor:,.2f}"

        # Troca . por vírgula (decimal) e , por ponto (milhares)
        valor_str = valor_str.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')

        return valor_str

    def atualizar_campos_imoveis(self, lead: Dict, imoveis: List[Dict]) -> bool:
        """Atualiza os 32 campos de imóveis do contato no Mailchimp (8 campos × 4 imóveis)"""
        try:
            subscriber_hash = hashlib.md5(lead['email'].lower().encode()).hexdigest()

            # Cria merge_fields para cada imóvel (máximo 4)
            merge_fields = {
                "FNAME": lead.get('nome', '').split(' ')[0] if lead.get('nome') else 'Cliente',
                "LNAME": lead.get('nome', '').split(' ', 1)[1] if lead.get('nome') and len(lead.get('nome', '').split(' ')) > 1 else '',
                "PHONE": lead.get('telefone', '') or '',
            }

            # Adiciona até 4 imóveis com 8 campos cada
            for i in range(4):
                if i < len(imoveis):
                    imo = imoveis[i]

                    # Valida se tem todos os campos obrigatórios
                    titulo = imo.get('TituloSite', '').strip()
                    endereco = imo.get('Endereco', '').strip()
                    codigo = str(imo.get('Codigo', '')).strip()
                    foto = str(imo.get('Foto', '')).strip()
                    valor = imo.get('ValorVenda')
                    dorm = imo.get('Dormitorios')
                    banh = imo.get('Banheiros')
                    vagas = imo.get('Vagas')

                    # Valida se tem URL de foto (campo texto agora, não URL)
                    if not foto or not foto.startswith('http'):
                        foto = 'https://via.placeholder.com/300x180?text=Sem+Foto'

                    # Se faltar qualquer campo obrigatório, pula este imóvel
                    if not titulo or not codigo or valor is None:
                        logger.warning(f"Imóvel {i+1} (cod: {codigo}) ignorado - campos obrigatórios faltando")
                        continue

                    # 8 campos por imóvel
                    merge_fields[f"IM{i+1}_TITULO"] = titulo[:255]
                    merge_fields[f"IM{i+1}_ENDER"] = endereco[:255]
                    merge_fields[f"IM{i+1}_COD"] = codigo[:255]
                    merge_fields[f"IM{i+1}_FOTO"] = foto[:255]
                    merge_fields[f"IM{i+1}_VALOR"] = self.formatar_valor_br(valor)[:255]
                    merge_fields[f"IM{i+1}_DORM"] = str(dorm if dorm is not None else 0)[:255]
                    merge_fields[f"IM{i+1}_BANH"] = str(banh if banh is not None else 0)[:255]
                    merge_fields[f"IM{i+1}_VAGAS"] = str(vagas if vagas is not None else 0)[:255]

            # Fluxo sem upsert: tenta buscar; se não existir cria; se existir atualiza
            try:
                self.mailchimp_client.lists.get_list_member(self.list_id, subscriber_hash)
                existe = True
            except Exception:
                existe = False

            if not existe:
                member_data = {
                    "email_address": lead['email'],
                    "status": "subscribed",
                    "merge_fields": merge_fields
                }
                try:
                    self.mailchimp_client.lists.add_list_member(self.list_id, member_data)
                    logger.info(f"Contato criado: {lead['email']}")
                except ApiClientError as e:
                    # Se já existir (ex.: arquivado), faz update para reativar/atualizar
                    try:
                        detail = json.loads(getattr(e, 'text', '{}'))
                    except Exception:
                        detail = {}
                    if detail.get('title') == 'Member Exists' or detail.get('status') == 400:
                        self.mailchimp_client.lists.update_list_member(
                            self.list_id,
                            subscriber_hash,
                            {"status": "subscribed", "merge_fields": merge_fields}
                        )
                        logger.info(f"Contato reativado/atualizado: {lead['email']}")
                    else:
                        raise
            else:
                self.mailchimp_client.lists.update_list_member(
                    self.list_id,
                    subscriber_hash,
                    {"merge_fields": merge_fields}
                )
                logger.info(f"Contato atualizado: {lead['email']}")

            return True

        except ApiClientError as e:
            logger.error(f"Erro ao atualizar {lead['email']}: {e.text}")
            return False

    def executar(self):
        """Executa o processo completo"""
        logger.info("Iniciando criação de campanha...")

        # Cria tag única para esta campanha
        tag_campanha = f"CAMP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Tag da campanha: {tag_campanha}")

        # Busca leads
        leads = self.get_leads_from_database()

        if not leads:
            logger.info("Nenhum lead encontrado")
            return

        # Estatísticas e lista de emails processados
        stats = {'total': len(leads), 'atualizados': 0, 'skipped': 0, 'errors': 0}
        emails_processados = []

        # Carrega caches antes do processamento
        self.load_all_imoveis()
        self.load_all_coordenadas()

        # Processamento concorrente dos leads (IO-bound no Mailchimp)
        def processar_um(lead: Dict):
            codigo = lead.get('mkt_produto_formatado')
            if not codigo:
                return ('skip', lead['email'], 'Sem código')

            logger.info(f"Processando lead {lead['email']} - Imóvel: {codigo}")
            imovel = self.get_imovel_data(codigo)
            if not imovel:
                return ('skip', lead['email'], f"Imóvel {codigo} não encontrado")

            semelhantes = self.get_imoveis_semelhantes(imovel)
            if not semelhantes:
                return ('skip', lead['email'], f"Sem imóveis semelhantes para {codigo}")

            ok = self.atualizar_campos_imoveis(lead, semelhantes)
            if not ok:
                return ('erro', lead['email'], 'Falha ao atualizar Mailchimp')

            # Adiciona tag única desta campanha ao contato
            subscriber_hash_local = hashlib.md5(lead['email'].lower().encode()).hexdigest()
            try:
                self.mailchimp_client.lists.update_list_member_tags(
                    self.list_id,
                    subscriber_hash_local,
                    {"tags": [{"name": tag_campanha, "status": "active"}]}
                )
                return ('ok', lead['email'], 'Atualizado e tag aplicado')
            except ApiClientError as e:
                logger.error(f"Erro ao adicionar tag: {e.text}")
                return ('ok', lead['email'], 'Atualizado (tag falhou)')

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(processar_um, lead) for lead in leads]
            for fut in as_completed(futures):
                status, email, _ = fut.result()
                if status == 'ok':
                    stats['atualizados'] += 1
                    emails_processados.append(email)
                elif status == 'skip':
                    stats['skipped'] += 1
                else:
                    stats['errors'] += 1

        # Log final
        logger.info("=" * 60)
        logger.info("Atualização concluída!")
        logger.info(f"Total: {stats['total']}")
        logger.info(f"Atualizados: {stats['atualizados']}")
        logger.info(f"Pulados: {stats['skipped']}")
        logger.info(f"Erros: {stats['errors']}")
        logger.info("=" * 60)

        # Cria campanha se houver leads atualizados
        if stats['atualizados'] > 0 and emails_processados:
            logger.info(f"Criando campanha no Mailchimp para {len(emails_processados)} contatos...")
            campaign_id = self.criar_campanha(emails_processados, tag_campanha)

            if campaign_id:
                logger.info(f"✅ Processo concluído!")
            else:
                logger.error("❌ Erro ao criar campanha")
        else:
            logger.warning("Nenhum lead atualizado. Campanha não criada.")


def main():
    """Função principal"""
    try:
        app = MailchimpCampanha()
        app.executar()

    except Exception as e:
        logger.error(f"Erro durante execução: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()