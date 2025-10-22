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
              AND DATE(bitrix_data) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
              AND mkt_produto REGEXP '[0-9]'
              AND email = 'edio.ferreira@icloud.com';
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
        """Busca dados do imóvel"""
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_imoveis_config)
            cursor = connection.cursor(dictionary=True)

            query = """
            SELECT *
            FROM tb_imoveis
            WHERE Codigo = %s;
            """

            cursor.execute(query, (codigo,))
            return cursor.fetchone()

        except Error as e:
            logger.error(f"Erro ao buscar imóvel {codigo}: {e}")
            return None

        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_coordenadas_imovel(self, codigo: str) -> tuple:
        """Busca latitude e longitude do imóvel na tabela agenciamentos do banco Leads"""
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)  # Banco de Leads
            cursor = connection.cursor(dictionary=True)

            query = """
            SELECT latitude, longitude
            FROM agenciamentos
            WHERE codigo_imovel = %s
            LIMIT 1;
            """

            cursor.execute(query, (codigo,))
            result = cursor.fetchone()

            if result and result.get('latitude') and result.get('longitude'):
                return (result['latitude'], result['longitude'])

            return (None, None)

        except Error as e:
            logger.error(f"Erro ao buscar coordenadas do imóvel {codigo}: {e}")
            return (None, None)

        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_imoveis_semelhantes(self, imovel: Dict) -> List[Dict]:
        """Busca imóveis semelhantes usando raio de 3km (latitude/longitude) ou BairroComercial como fallback"""
        connection = None
        try:
            # Busca coordenadas do imóvel principal
            lat_origem, lon_origem = self.get_coordenadas_imovel(imovel['Codigo'])

            connection = mysql.connector.connect(**self.db_imoveis_config)
            cursor = connection.cursor(dictionary=True)

            # Se tiver coordenadas, usa raio de 3km
            if lat_origem and lon_origem:
                logger.info(f"Buscando imóveis semelhantes por localização (3km) para {imovel['Codigo']}")

                # Fórmula de Haversine para calcular distância em km
                # 3km de raio, área ±20%, valor ±20%
                query = """
                SELECT tb_imoveis.*,
                       (6371 * acos(
                           cos(radians(%s)) *
                           cos(radians(a.latitude)) *
                           cos(radians(a.longitude) - radians(%s)) +
                           sin(radians(%s)) *
                           sin(radians(a.latitude))
                       )) AS distancia
                FROM tb_imoveis
                INNER JOIN agenciamentos a ON tb_imoveis.Codigo = a.codigo_imovel
                WHERE tb_imoveis.Dormitorios = %s
                  AND tb_imoveis.AreaPrivativa BETWEEN (%s * 0.8) AND (%s * 1.2)
                  AND tb_imoveis.ValorVenda BETWEEN (%s * 0.8) AND (%s * 1.2)
                  AND tb_imoveis.Codigo != %s
                  AND tb_imoveis.Foto IS NOT NULL
                  AND tb_imoveis.TituloSite IS NOT NULL
                  AND a.latitude IS NOT NULL
                  AND a.longitude IS NOT NULL
                HAVING distancia <= 3
                ORDER BY distancia ASC
                LIMIT 4;
                """

                params = (
                    lat_origem, lon_origem, lat_origem,  # Para Haversine
                    imovel['Dormitorios'],
                    imovel['AreaPrivativa'], imovel['AreaPrivativa'],
                    imovel['ValorVenda'], imovel['ValorVenda'],
                    imovel['Codigo']
                )
            else:
                # Fallback: usa BairroComercial
                logger.warning(f"Imóvel {imovel['Codigo']} sem coordenadas, usando BairroComercial como fallback")

                query = """
                SELECT *
                FROM tb_imoveis
                WHERE Dormitorios = %s
                  AND AreaPrivativa BETWEEN (%s * 0.8) AND (%s * 1.2)
                  AND ValorVenda BETWEEN (%s * 0.8) AND (%s * 1.2)
                  AND BairroComercial = %s
                  AND Codigo != %s
                  AND Foto IS NOT NULL
                  AND TituloSite IS NOT NULL
                LIMIT 4;
                """

                params = (
                    imovel['Dormitorios'],
                    imovel['AreaPrivativa'], imovel['AreaPrivativa'],
                    imovel['ValorVenda'], imovel['ValorVenda'],
                    imovel['BairroComercial'],
                    imovel['Codigo']
                )

            cursor.execute(query, params)
            return cursor.fetchall()

        except Error as e:
            logger.error(f"Erro ao buscar imóveis semelhantes: {e}")
            return []

        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

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

            # Verifica se existe
            try:
                subscriber_hash_check = hashlib.md5(lead['email'].lower().encode()).hexdigest()
                self.mailchimp_client.lists.get_list_member(self.list_id, subscriber_hash_check)
                existe = True
            except:
                existe = False

            if not existe:
                # Cria novo contato (tag será adicionada depois)
                member_data = {
                    "email_address": lead['email'],
                    "status": "subscribed",
                    "merge_fields": merge_fields
                }
                self.mailchimp_client.lists.add_list_member(self.list_id, member_data)
                logger.info(f"Contato criado: {lead['email']}")
            else:
                # Atualiza existente
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

        # Para cada lead, busca imóveis semelhantes e atualiza no Mailchimp
        for lead in leads:
            codigo = lead.get('mkt_produto_formatado')

            if not codigo:
                logger.warning(f"Lead {lead['email']} sem código de imóvel")
                stats['skipped'] += 1
                continue

            logger.info(f"Processando lead {lead['email']} - Imóvel: {codigo}")

            # Busca imóvel
            imovel = self.get_imovel_data(codigo)

            if not imovel:
                logger.warning(f"Imóvel {codigo} não encontrado")
                stats['skipped'] += 1
                continue

            # Busca semelhantes
            semelhantes = self.get_imoveis_semelhantes(imovel)

            if not semelhantes:
                logger.warning(f"Sem imóveis semelhantes para {codigo}")
                stats['skipped'] += 1
                continue

            # Atualiza campos no Mailchimp
            if self.atualizar_campos_imoveis(lead, semelhantes):
                stats['atualizados'] += 1
                emails_processados.append(lead['email'])

                # Adiciona tag única desta campanha ao contato
                subscriber_hash = hashlib.md5(lead['email'].lower().encode()).hexdigest()
                try:
                    self.mailchimp_client.lists.update_list_member_tags(
                        self.list_id,
                        subscriber_hash,
                        {"tags": [{"name": tag_campanha, "status": "active"}]}
                    )
                    logger.info(f"Tag {tag_campanha} adicionada ao contato: {lead['email']}")
                except ApiClientError as e:
                    logger.error(f"Erro ao adicionar tag: {e.text}")
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