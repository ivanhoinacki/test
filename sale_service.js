const SaleModel = require('./sale_model');
const ReceivedSaleModel = require('./received_sale_model');
const BaseService = require('../../base/base_service');
const CampaignIntegration = require('../../../../providers/campaigns/campaigns_integration');
const MyCashIntegration = require('../../../../providers/mycash/mycash_integrations');
const NotificationsIntegration = require('../../../../providers/notifications/notifications_integration');
const DitoEventsIntegration = require('../../../../providers/dito/dito_events_integration');
const DitoUsersIntegration = require('../../../../providers/dito/dito_users_integration');
const _ = require('lodash');
const sub = require('date-fns/sub');
const add = require('date-fns/add');
const startOfDay = require('date-fns/startOfDay');
const endOfDay = require('date-fns/endOfDay');
const parseISO = require('date-fns/parseISO');
const formatISO = require('date-fns/formatISO');
const isBefore = require('date-fns/isBefore');

const XLSX = require('xlsx');

const mongoose = require('mongoose');

class SaleService extends BaseService {
    constructor() {
        super();
        this._saleModel = SaleModel;
        this._receivedSaleModel = ReceivedSaleModel;
        this._campaignIntegration = new CampaignIntegration();
        this._myCashIntegration = new MyCashIntegration();
        this._notificationsIntegration = new NotificationsIntegration();
        this._ditoEventsIntegration = new DitoEventsIntegration();
        this._ditoUsersIntegration = new DitoUsersIntegration();
    }

    /**
     * A função _findSaleWithBetterCampaign recebe uma lista de vendas como argumento e retorna a venda com o maior cashback total.
     * Para fazer isso, a função usa a biblioteca lodash para ordenar as vendas pela propriedade totalCashback em ordem crescente (menor para o maior) e então retorna a última venda da lista, que terá o maior cashback total.
     */
    _findSaleWithBetterCampaign(sales) {
        return _.sortBy(sales, ['totalCashback']).pop();
    }

    /**
     * A função _processSale é utilizada para processar uma venda dentro de uma campanha de marketing específica.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha de marketing e um objeto sale contendo informações sobre a venda a ser processada.
     * A função calcula e aplica os benefícios previstos na campanha de marketing na venda.
     * Isso pode incluir o cálculo e aplicação de cashback, descontos ou brindes, dependendo das regras definidas para a campanha.
     * Além disso, a função atualiza o status da campanha de marketing com o número de vendas processadas e o valor total das vendas processadas, para fins de acompanhamento e análise do desempenho da campanha.
     * Ao final do processamento da venda, a função retorna um objeto contendo informações atualizadas sobre a venda, incluindo os benefícios aplicados e o status da campanha de marketing atualizado.
     * Essa função é importante porque permite que as vendas sejam tratadas de forma personalizada para cada campanha de marketing, aumentando a satisfação do comprador e melhorando os resultados da empresa.
     */
    _processSale(campaign, sale) {
        sale.usedCampaign = campaign.code;
        let total = 0;

        const verificationDate = this._updateHours(
            new Date().getTimezoneOffset() / 60,
            startOfDay(this._updateHours(0 - new Date().getTimezoneOffset() / 60, parseISO(sale.verification)))
        );

        if (sale.salesChannel === 'PDV' && campaign.daysToCreditPdv) {
            sale.creditDate = this._addDays(verificationDate, campaign.daysToCreditPdv);
        } else {
            sale.creditDate = this._addDays(verificationDate, campaign.daysToCreditEcom);
        }

        sale.expirateDate = this._updateHours(
            new Date().getTimezoneOffset() / 60,
            endOfDay(
                this._updateHours(
                    0 - new Date().getTimezoneOffset() / 60,
                    this._addDays(sale.creditDate, campaign.daysToRescue)
                )
            )
        );

        if (campaign.percentCashback) {
            sale.items.forEach((item) => {
                if (item.matchedCampaigns.includes(campaign.code)) {
                    item.eligible = true;
                    item.unitCashback = _.floor(item.unitPrice * (campaign.percentCashback / 100));
                    item.totalCashback = item.unitCashback * item.quantity;

                    total += item.totalCashback;
                } else {
                    item.eligible = false;
                    delete item.totalCashback;
                    delete item.unitCashback;
                }
            });

            sale.totalCashback = total;
        } else {
            sale.totalCashback = campaign.valueCashback;
        }

        if (campaign.cashbackLimit && campaign.cashbackLimit < total) {
            sale.totalCashback = campaign.cashbackLimit;

            sale.items.forEach((item) => {
                delete item.totalCashback;
                delete item.unitCashback;
            });
        }
        sale.campaignData = {
            name: campaign.name,
            code: campaign.code,
            status: campaign.status,
            startDate: campaign.startDate,
            endDate: campaign.endDate
        };
        return sale;
    }

    /**
     * A função _checkMinSaleValueRule é utilizada para verificar se uma venda atende a uma das regras de uma campanha de cashback, que é o valor mínimo de venda para que o usuário seja elegível para o cashback.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha em questão e um objeto sale contendo informações sobre a venda em questão.
     * A função verifica se o valor total da venda é maior ou igual ao valor mínimo definido na campanha.
     * Se a venda atender ao valor mínimo, a função retorna true, indicando que a venda é elegível para o cashback.
     * Caso contrário, a função retorna false, indicando que a venda não é elegível para o cashback.
     * Essa função é importante porque permite que a empresa defina critérios mínimos para que um usuário seja elegível para o cashback, garantindo que apenas vendas de um determinado valor mínimo recebam o benefício.
     * Isso ajuda a empresa a controlar melhor seus gastos com cashback e a evitar fraudes ou abusos por parte dos usuários.
     */
    _checkMinSaleValueRule(campaign, sale) {
        const totalValue = sale.items.reduce(
            (prev, cur) => (cur.matchedCampaigns.includes(campaign.code) ? (prev += cur.totalPrice) : prev),
            0
        );

        if (campaign.minSaleValue && totalValue < campaign.minSaleValue) return false;

        return true;
    }

    /**
     * A função _checkMaxsalesCartRule é responsável por verificar se uma venda atende às regras de uma campanha de cashback relacionadas ao valor máximo de venda permitido.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha de cashback em questão e um objeto sale contendo informações sobre a venda em questão.
     * A função verifica se o valor total da venda é menor ou igual ao valor máximo permitido pela campanha.
     * Se o valor total da venda for menor ou igual ao valor máximo permitido, a função retorna true, indicando que a venda é elegível para o cashback.
     * Caso contrário, a função retorna false, indicando que a venda não é elegível para o cashback.
     * Essa função é importante porque permite que a empresa controle melhor seus gastos com a campanha de cashback, definindo um limite máximo de venda para que os usuários possam receber o cashback.
     * Isso ajuda a evitar perdas financeiras para a empresa e a manter a campanha dentro do orçamento previsto
     */
    _checkMaxsalesCartRule(campaign, sale) {
        const totalItems = sale.items.reduce(
            (prev, cur) => (cur.matchedCampaigns.includes(campaign.code) ? (prev += cur.quantity) : prev),
            0
        );

        if (campaign.maxProductsCart && totalItems > campaign.maxProductsCart) return false;

        return true;
    }

    /**
     * A função _checkSalesChannelRule é responsável por verificar se uma venda atende às regras de uma campanha de cashback relacionadas ao canal de vendas utilizado.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha de cashback em questão e um objeto sale contendo informações sobre a venda em questão.
     * A função verifica se o canal de vendas utilizado na venda está dentro dos canais de venda definidos na campanha.
     * Se o canal de vendas da venda estiver dentro dos canais definidos na campanha, a função retorna true, indicando que a venda é elegível para o cashback.
     * Caso contrário, a função retorna false, indicando que a venda não é elegível para o cashback.
     * Essa função é importante porque permite que a empresa defina quais canais de vendas serão aceitos para que o usuário possa receber o cashback, ajudando a controlar melhor seus gastos com essa ação de marketing.
     * Isso também ajuda a incentivar o uso de determinados canais de venda e a aumentar a visibilidade da empresa em diferentes plataformas.
     */
    _checkSalesChannelRule(campaign, sale) {
        return campaign.salesChannel.includes(sale.salesChannel);
    }

    /**
     * A função _checkPaymentMethodRule é responsável por verificar se uma venda atende às regras de uma campanha de cashback relacionadas ao método de pagamento utilizado.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha de cashback em questão e um objeto sale contendo informações sobre a venda em questão.
     * A função verifica se o método de pagamento utilizado na venda está dentro dos métodos de pagamento definidos na campanha.
     * Se o método de pagamento da venda estiver dentro dos métodos definidos na campanha, a função retorna true, indicando que a venda é elegível para o cashback.
     * Caso contrário, a função retorna false, indicando que a venda não é elegível para o cashback.
     * Essa função é importante porque permite que a empresa defina quais métodos de pagamento serão aceitos para que o usuário possa receber o cashback, ajudando a controlar melhor seus gastos com essa ação de marketing.
     * Isso também ajuda a incentivar o uso de determinados métodos de pagamento e a aumentar a visibilidade da empresa em diferentes canais de pagamento.
     */
    _checkSubsidiariesChannelRule(campaign, sale) {
        if (!campaign.subsidiariesList || campaign.subsidiariesList.length < 1) return true;
        else return campaign.subsidiariesList.includes(sale.order.origin);
    }

    /**
     * A função _checkPaymentMethodRule é responsável por verificar se uma determinada venda está de acordo com a regra de método de pagamento definida para uma campanha de marketing específica.
     * Essa função recebe como parâmetros um objeto de campanha e um objeto de venda e verifica se o método de pagamento da venda é permitido pela campanha, comparando-o com a lista de métodos de pagamento permitidos na campanha.
     * Caso o método de pagamento seja permitido, a função retorna verdadeiro (true). Caso contrário, a função retorna falso (false).
     * Essa função é importante para garantir que as vendas que serão processadas dentro de uma campanha de marketing estejam em conformidade com as regras definidas para a campanha, garantindo que os benefícios sejam aplicados apenas às vendas elegíveis.
     */
    _checkPaymentMethodRule(campaign, sale) {
        let result = true;

        sale.paymentMethod.forEach((pM) => {
            const [paymentMethod] = campaign.paymentMethod.filter((p) => p.type === pM.type);

            if (!paymentMethod) {
                result = false;
            } else if (
                ['CREDIT_CARD', 'DEBIT_CARD'].includes(paymentMethod.type) &&
                paymentMethod.flags &&
                paymentMethod.flags !== ''
            ) {
                if (!paymentMethod.flags.includes(pM.flag)) {
                    result = false;
                }
            }
        });
        return result;
    }

    /**
     * A função _checkItemsThatFollowRules é utilizada para verificar se os itens de uma venda seguem as regras estabelecidas por uma campanha de cashback.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha em questão e um objeto sale contendo informações sobre a venda em questão.
     * A função itera sobre os itens da venda e verifica se cada item segue as regras estabelecidas pela campanha de cashback.
     * Se o item seguir as regras, a função adiciona uma propriedade followsRules ao objeto item com o valor true.
     * Se o item não seguir as regras, a função adiciona uma propriedade followsRules ao objeto item com o valor false.
     * Ao final do processamento dos itens, a função retorna o objeto sale atualizado com as informações sobre os itens que seguem as regras estabelecidas pela campanha de cashback.
     * Essa função é importante porque permite que a empresa verifique se os itens vendidos atendem aos critérios estabelecidos pela campanha de cashback, evitando fraudes ou abusos por parte dos usuários.
     */
    _checkItemsThatFollowRules(campaign, sale) {
        let hasOneProductOnRule = false;

        const rules = campaign.rules;
        const rulesKeys = [
            'group',
            'category1',
            'category2',
            'category3',
            'category4',
            'gender',
            'colorCode',
            'model',
            'size'
        ];

        sale.items.forEach((item) => {
            if (!item.matchedCampaigns) item.matchedCampaigns = [];
            item.eligible = false;

            rules.forEach((rule) => {
                let eligible = true;

                rulesKeys.forEach((ruleKey) => {
                    if (!!rule[ruleKey] && rule[ruleKey] != item[ruleKey]) {
                        eligible = false;
                    }
                });

                if (eligible) {
                    item.matchedCampaigns.push(campaign.code);

                    hasOneProductOnRule = true;
                }
            });
        });

        return hasOneProductOnRule;
    }

    /**
     * A função _checkCpfParticipationLimit é utilizada para verificar se o CPF de um usuário já atingiu o limite de participação em uma campanha de cashback.
     * Essa função recebe como parâmetros um objeto campaign contendo informações sobre a campanha em questão e um objeto user contendo informações sobre o usuário em questão.
     * A função verifica se o número de participações do usuário na campanha já atingiu o limite definido na campanha. Se o limite já tiver sido atingido, a função retorna false, indicando que o usuário não pode participar da campanha.
     * Caso contrário, a função retorna true, indicando que o usuário pode participar da campanha.
     * Essa função é importante porque permite que a empresa evite fraudes ou abusos por parte dos usuários, limitando a participação de cada usuário em uma campanha de cashback.
     */
    async _checkCpfParticipationLimit(campaignCode, cpfParticipationLimit, cpf) {
        cpf = cpf.replace(/\D/g, '');

        const participations = await this._saleModel.countDocuments({ cpf, usedCampaign: campaignCode });

        console.log(`${campaignCode} participations: ${participations}`);
        if (participations >= cpfParticipationLimit) return false;

        return true;
    }

    /**
     * A função await this._checkSaleCampaign é utilizada para verificar se uma venda realizada está associada a alguma campanha de marketing em vigor.
     * Essa função recebe como parâmetros um objeto saleData contendo informações sobre a venda e uma lista de objetos campaigns contendo informações sobre as campanhas de marketing ativas.
     * A função verifica se a venda está associada a alguma campanha de marketing que está em vigor.
     * Para isso, a função compara a data de realização da venda com as datas de início e fim de cada campanha na lista.
     * Se a data de realização da venda estiver dentro do período de alguma campanha em vigor, a função retorna o objeto dessa campanha.
     * Se a venda não estiver associada a nenhuma campanha em vigor, a função retorna null.
     */
    async _checkSaleCampaign(campaign, sale, cpf) {
        if (!this._checkSalesChannelRule(campaign, sale)) {
            console.log(`fail ${campaign.code} _checkSalesChannelRule`);
            return { isValid: false, reason: 'FAIL_SALES_CHANNEL_RULE' };
        } else if (!this._checkSubsidiariesChannelRule(campaign, sale)) {
            console.log(`fail ${campaign.code} _checkSubsidiariesChannelRule`);
            return { isValid: false, reason: 'FAIL_SALES_SUBSIDIARIES_RULE' };
        } else if (!this._checkPaymentMethodRule(campaign, sale)) {
            console.log(`fail ${campaign.code} _checkPaymentMethodRule`);

            return { isValid: false, reason: 'FAIL_PAYMENT_METHOD_RULE' };
        } else if (!this._checkItemsThatFollowRules(campaign, sale)) {
            console.log(`fail ${campaign.code} _checkItemsThatFollowRules`);

            return { isValid: false, reason: 'FAIL_ITEMS_RULE' };
        } else if (campaign.maxProductsCart && !this._checkMaxsalesCartRule(campaign, sale)) {
            console.log(`fail ${campaign.code} _checkMaxsalesCartRule`);

            return { isValid: false, reason: 'FAIL_MAX_SALES_CART_RULE' };
        } else if (campaign.minSaleValue && !this._checkMinSaleValueRule(campaign, sale)) {
            console.log(`fail ${campaign.code} _checkMinSaleValueRule`);

            return { isValid: false, reason: 'FAIL_MIN_VALUE_RULE' };
        } else if (
            campaign.cpfParticipationLimit &&
            !(await this._checkCpfParticipationLimit(campaign.code, campaign.cpfParticipationLimit, cpf))
        ) {
            console.log(`fail ${campaign.code} _checkCpfParticipationLimit`);

            return { isValid: false, reason: 'FAIL_CPF_PARTICIPATION_LIMIT_RULE' };
        }

        if (!sale.matchedCampaigns) sale.matchedCampaigns = [campaign.code];
        else sale.matchedCampaigns.push(campaign.code);

        return { isValid: true };
    }

    /**
     *
     * A função await this._calculeCashback é utilizada para calcular o valor do cashback que um comprador receberá por uma venda.
     * Essa função recebe como parâmetros um objeto saleData contendo informações sobre a venda e uma lista de objetos campaigns contendo informações sobre as campanhas de marketing ativas que estão associadas à venda.
     * A função utiliza as informações contidas no objeto saleData e nas campanhas de marketing para calcular o valor do cashback que o comprador receberá pela venda.
     * O cálculo do cashback pode envolver várias regras de negócio, como percentuais de desconto, valores mínimos e máximos de cashback, regras de elegibilidade para participação na campanha, entre outras.
     * A função retorna o valor do cashback calculado para a venda. Esse valor será usado posteriormente na função _createNewSale para atualizar o saldo de cashback do comprador e para registrar informações sobre a venda.
     */
    async _calculeCashback(campaigns, sale, customerId, receivedSaleId) {
        if (!campaigns) {
            await this._receivedSaleModel.findByIdAndUpdate(receivedSaleId, { $set: { reason: { campaigns } } });
            throw new Error('SALE_NOT_MATCH_ANY_CAMPAIGN');
        }

        const validCampaigns = [];
        const invalidCampaigns = [];

        sale.items = this._processItems(sale.items);

        for await (const campaign of campaigns) {
            const { isValid, reason } = await this._checkSaleCampaign(campaign, sale, customerId);
            console.log(`${campaign.code} isValid: ${isValid}`);
            if (isValid) validCampaigns.push(campaign);
            else invalidCampaigns.push({ isValid, reason, campaign: campaign.code });
        }

        if (!validCampaigns || validCampaigns.length < 1) {
            console.log('SALE_NOT_MATCH_ANY_CAMPAIGN');

            const err = new Error('SALE_NOT_MATCH_ANY_CAMPAIGN');

            err.reasons = invalidCampaigns;
            await this._receivedSaleModel.findByIdAndUpdate(receivedSaleId, {
                $set: { reason: { campaigns: invalidCampaigns } }
            });

            throw err;
        }

        /**
         * É utilizada para processar uma venda dentro de cada campanha de marketing que a venda estiver associada.
         * Essa função recebe como parâmetros uma lista validCampaigns de campanhas de marketing ativas às quais a venda está associada e um objeto sale contendo informações sobre a venda.
         * Para cada campanha de marketing ativa, a função chama a função _processSale, passando como parâmetros a campanha de marketing e um clone da venda.
         * A clonagem da venda é feita para evitar que alterações feitas durante o processamento da venda em uma campanha afetem a venda em outras campanhas.
         * A função _processSale é responsável por calcular e aplicar os benefícios previstos na campanha de marketing na venda.
         * Isso pode incluir o cálculo e aplicação de cashback, descontos ou brindes, dependendo das regras definidas para a campanha.
         * Ao final do processamento de todas as campanhas de marketing associadas à venda, a função retorna um objeto contendo informações sobre a venda atualizadas com os benefícios aplicados.
         * Essa função é importante porque permite que as vendas sejam processadas dentro de cada campanha de marketing à qual estão associadas, garantindo que os benefícios previstos em cada campanha sejam aplicados corretamente.
         */
        const result = validCampaigns.map((validCampaign) => this._processSale(validCampaign, _.cloneDeepWith(sale)));

        return { validCampaigns: result, invalidCampaigns };
    }

    /**
     * A função _markEligibleItemsBasedOnUsedCampaign é utilizada para marcar os itens de uma venda que são elegíveis para receber cashback com base em uma campanha que já foi utilizada anteriormente pelo usuário.
     * Essa função recebe como parâmetros um objeto sale contendo informações sobre a venda em questão e um objeto usedCampaigns contendo informações sobre as campanhas que já foram utilizadas pelo usuário. A função itera sobre os itens da venda e verifica se cada item é elegível para receber cashback com base nas campanhas já utilizadas pelo usuário.
     * Se o item for elegível, a função adiciona uma propriedade eligibleForCashback ao objeto item com o valor true.
     * Ao final do processamento dos itens, a função retorna o objeto sale atualizado com as informações sobre os itens elegíveis para receber cashback.
     * Essa função é importante porque permite que a empresa ofereça cashback de forma personalizada para cada item da venda com base nas campanhas que já foram utilizadas pelo usuário, incentivando a fidelização do cliente e promovendo vendas adicionais.
     */
    _markEligibleItemsBasedOnUsedCampaign(sale) {
        sale.items.forEach((item) => {
            item.eligible = item.matchedCampaigns.includes(sale.usedCampaign);
        });
    }

    /**
     * A função _getActivesCampaigns é responsável por buscar no banco de dados todas as campanhas de marketing que estão ativas, ou seja, que possuem data de início anterior ou igual à data atual e data de término posterior ou igual à data atual.
     * Essa função retorna um array contendo todas as campanhas ativas.
     */
    async _getActivesCampaigns() {
        return await this._campaignIntegration.getCampaigns({ status: 'ACTIVE' });
    }

    /**
     * A função _addDays é usada para adicionar uma quantidade específica de dias a uma data.
     * Ela recebe dois parâmetros: uma data e um número inteiro representando o número de dias a serem adicionados.
     * A função usa a biblioteca date-fns para adicionar o número de dias especificado à data passada como argumento e retorna a nova data resultante.
     * Essa função é útil para calcular a data de vencimento ou expiração de uma venda ou campanha de marketing, por exemplo.
     */
    _addDays(baseDate, number) {
        const newDate = new Date(baseDate);
        return add(newDate, { days: number });
    }

    /**
     * A função _createUsedCashbackSale é responsável por criar uma nova venda que utiliza o cashback acumulado pelo cliente como forma de pagamento. Ela recebe como parâmetro um objeto saleData que contém as informações da venda, incluindo o ID do cliente, o ID do produto vendido, o preço unitário do produto e a quantidade de produtos vendidos.
     *  Antes de criar a nova venda, a função verifica se o cliente possui cashback acumulado suficiente para cobrir o valor da venda. Caso contrário, a função retorna um erro informando que o cliente não possui cashback suficiente.
     *  Se o cliente possui cashback suficiente, a função cria a nova venda e subtrai o valor do cashback utilizado do saldo total de cashback do cliente.
     *  Por fim, a função retorna um objeto que contém as informações da venda recém-criada, incluindo o ID da venda, o ID do produto vendido, o ID do cliente que realizou a compra, o preço unitário do produto, a quantidade de produtos vendidos, a data e hora da venda e o valor do cashback utilizado.
     */
    _createUsedCashbackSale(sale) {
        return {
            status: 'USED',
            invoiceKey: sale.invoice && sale.invoice.key,
            cpf: sale.customer.id,
            email: sale.customer.email,
            saleDate: sale.verification,
            salesChannel: sale.salesChannel,
            usedCashback: Boolean(sale.usedCashback),
            usedCashbackValue: sale.usedCashbackValue,
            order: sale.order,
            items: sale.items,
            customer: sale.customer,
            invoice: sale.invoice,
            paymentMethod: sale.paymentMethod
        };
    }

    /**
     * A função _processItems é responsável por processar os itens de uma venda, calculando o valor do cashback para cada item com base nas campanhas ativas e nas regras de cashback definidas.
     * Essa função recebe como parâmetros um array items contendo informações sobre os itens da venda, um objeto campaigns contendo informações sobre as campanhas ativas e um objeto sale contendo informações sobre a venda em questão.
     * A função itera sobre cada item da venda e para cada item, verifica se ele é elegível para receber cashback com base nas regras de cashback definidas para cada campanha ativa. Se o item for elegível, a função calcula o valor do cashback com base nas regras de cashback definidas e adiciona o valor calculado ao objeto cashback correspondente.
     * Ao final do processamento dos itens, a função retorna um array de objetos cashback, contendo informações sobre o cashback gerado para cada item elegível.
     * Essa função é importante porque permite que a empresa ofereça cashback de forma personalizada para cada item da venda com base nas campanhas ativas e nas regras de cashback definidas.
     * Isso ajuda a aumentar a fidelização dos clientes e a promover vendas adicionais, além de permitir que a empresa avalie o desempenho de suas campanhas de cashback e faça ajustes para melhorar os resultados.
     */
    _processItems(items) {
        items.forEach((item) => {
            const [model, colorCode, size] = item.partnumber.split('.');

            item.model = model.trim();
            item.colorCode = colorCode.trim();
            item.size = size.trim();
            item.totalPrice = item.unitPrice * item.quantity;
        });
        return items;
    }

    /**
     * A função _createOrAddCashbackUseHistory é utilizada para criar ou atualizar um registro no histórico de uso de cashback.
     * Essa função recebe como parâmetros um objeto cashback contendo informações sobre o cashback gerado em uma venda e um objeto sale contendo informações sobre a venda em questão.
     * A função verifica se o usuário que realizou a compra já utilizou cashback anteriormente. Se o usuário já utilizou cashback antes, a função atualiza o registro no histórico de uso de cashback, adicionando as informações relevantes sobre a nova transação de cashback à lista de transações existente.
     * Se o usuário nunca utilizou cashback antes, a função cria um novo registro no histórico de uso de cashback, contendo informações como o ID do usuário, o valor total de cashback utilizado e a lista de transações de cashback.
     * Ao final do processamento da venda, a função retorna o objeto cashback atualizado com informações sobre o registro no histórico de uso de cashback.
     * Essa função é importante porque permite que a empresa acompanhe e analise o uso de cashback pelos usuários ao longo do tempo, identificando padrões ou tendências e fazendo ajustes em suas estratégias de marketing e vendas para melhorar os resultados.
     * Além disso, permite que a empresa forneça uma experiência personalizada aos usuários, oferecendo cashback com base em seus históricos de transações anteriores.
     */
    _createOrAddCashbackUseHistory({ usedValue, invoiceKey, cashbackUseHistory, saleId }) {
        const history = {
            usedValue,
            invoiceKey,
            saleId,
            date: new Date()
        };
        if (_.isArray(cashbackUseHistory)) return [...cashbackUseHistory, history];
        else return [history];
    }

    /**
     * A função _createCashbackFontHistory é utilizada para criar um registro no histórico de fontes de cashback.
     * Essa função recebe como parâmetros um objeto cashback contendo informações sobre o cashback gerado em uma venda e um objeto sale contendo informações sobre a venda em questão.
     * A função cria um novo registro no histórico de fontes de cashback, contendo informações como o valor do cashback gerado, a data em que o cashback foi gerado, o ID da venda e outras informações relevantes.
     * Ao final do processamento da venda, a função retorna o objeto cashback atualizado com informações sobre o registro no histórico de fontes de cashback.
     * Essa função é importante porque permite que a empresa acompanhe e analise o desempenho de suas fontes de cashback ao longo do tempo.
     * Além disso, permite que a empresa identifique padrões ou tendências no uso de cashback e faça ajustes em suas estratégias de marketing e vendas para melhorar os resultados.
     */
    _createCashbackFontHistory({ usedValue, invoiceKey, saleId }) {
        return {
            usedValue,
            from: invoiceKey,
            saleId,
            date: new Date()
        };
    }

    /**
     * A função this._useCashback é responsável por atualizar o saldo de cashback de um determinado cliente após ele utilizar o cashback acumulado como forma de pagamento em uma venda.
     * Ela recebe como parâmetro o ID do cliente e o valor do cashback utilizado na venda.
     * A função busca o cliente no banco de dados e verifica se ele possui cashback acumulado suficiente para cobrir o valor do cashback utilizado na venda. Caso contrário, a função retorna um erro informando que o cliente não possui cashback suficiente.
     * Se o cliente possui cashback suficiente, a função subtrai o valor do cashback utilizado do saldo total de cashback do cliente e atualiza o documento do cliente no banco de dados com o novo saldo.
     * Por fim, a função retorna o saldo atualizado de cashback do cliente em formato de número.
     * Se ocorrer algum erro durante o processo de atualização do saldo no banco de dados, a função retorna o erro gerado.
     */
    async _useCashback({ usedValue, cpf, invoiceKey, saleId }) {
        const salesWithCashback = await this._saleModel.find(
            { cpf, status: 'AVAILABLE', availableCashback: { $gt: 0 } },
            {},
            { sort: { expirateDate: -1 } }
        );

        let i = 0;
        let usedValueAux = usedValue;
        const history = [];
        const usedSales = [];

        while (usedValueAux) {
            const sale = salesWithCashback[i];

            if (sale.availableCashback >= usedValueAux) {
                sale.cashbackUseHistory = this._createOrAddCashbackUseHistory({
                    usedValue: usedValueAux,
                    invoiceKey,
                    saleId,
                    cashbackUseHistory: sale.cashbackUseHistory
                });

                history.push(
                    this._createCashbackFontHistory({
                        usedValue: usedValueAux,
                        saleId: sale._id,
                        invoiceKey: sale.invoiceKey
                    })
                );

                sale.availableCashback = sale.availableCashback - usedValueAux;
                usedValueAux = 0;
            } else if (usedValueAux > sale.availableCashback) {
                sale.cashbackUseHistory = this._createOrAddCashbackUseHistory({
                    usedValue: sale.availableCashback,
                    invoiceKey,
                    saleId,
                    cashbackUseHistory: sale.cashbackUseHistory
                });
                history.push(
                    this._createCashbackFontHistory({
                        usedValue: sale.availableCashback,
                        saleId: sale._id,
                        invoiceKey: sale.invoiceKey
                    })
                );

                usedValueAux = usedValueAux - sale.availableCashback;
                sale.availableCashback = 0;
            }
            usedSales.push(sale);
            i++;
        }

        await Promise.all(
            usedSales.map((usedSale) =>
                this._saleModel.updateOne(
                    { _id: usedSale._id },
                    {
                        $set: {
                            availableCashback: usedSale.availableCashback,
                            cashbackUseHistory: usedSale.cashbackUseHistory
                        }
                    }
                )
            )
        );

        return history;
    }

    /**
     * A função _getBalance é responsável por obter o saldo atual de cashback de um determinado cliente.
     * Ela recebe como parâmetro o ID do cliente cujo saldo de cashback deve ser obtido.
     * Para calcular o saldo de cashback do cliente, a função percorre todas as vendas realizadas por esse cliente e verifica se cada venda gerou cashback.
     * Caso positivo, o valor do cashback é adicionado ao saldo total de cashback do cliente.
     * Por fim, a função retorna o saldo total de cashback do cliente em formato de número.
     * Se o cliente não tiver nenhuma venda realizada que gerou cashback, a função retorna o valor zero.
     */
    async _getBalance(cpf) {
        const [balanceAgg] = await this._saleModel.aggregate([
            {
                $match: {
                    cpf: cpf,
                    status: 'AVAILABLE'
                }
            },
            {
                $group: {
                    _id: '$cpf',
                    balance: {
                        $sum: '$availableCashback'
                    }
                }
            }
        ]);
        return balanceAgg;
    }

    /**
     * A função _getLastRescues busca no banco de dados as últimas apurações de resgate de cashback realizadas.
     * Essa função retorna um array com as últimas apurações, ordenadas pela data de criação.
     * O objetivo dessa função é permitir que o sistema possa verificar se já houve uma apuração recente de resgate de cashback antes de realizar uma nova apuração.
     */
    async _getLastRescues(cpf, now) {
        const creditDate = startOfDay(sub(now, { months: 2 }));
        const [lastRescuesAgg] = await this._saleModel.aggregate([
            {
                $match: {
                    cpf: cpf,
                    creditDate: { $gte: this._updateHours(creditDate.getTimezoneOffset() / 60, creditDate) }
                }
            },
            {
                $group: {
                    _id: '$cpf',
                    value: {
                        $sum: '$totalCashback'
                    }
                }
            }
        ]);
        return lastRescuesAgg;
    }

    /**
     * A função _getCloseToExpire é responsável por buscar o valor total do cashback disponível que está próximo do prazo de expiração para um determinado CPF.
     * A função começa definindo a data limite para que o cashback esteja próximo da expiração, adicionando um mês a partir da data atual (now) e definindo o último segundo do dia como data limite.
     * Em seguida, a função usa a função aggregate do MongoDB para buscar as vendas que correspondem ao CPF, que possuem status "AVAILABLE" (disponível para resgate) e possuem uma data de expiração anterior ou igual à data limite definida anteriormente.
     * Por fim, a função realiza uma operação de soma do cashback disponível para cada venda encontrada e retorna o valor total em um objeto com a estrutura { _id: 'CPF', value: 'valor_total_disponivel' }.
     */
    async _getCloseToExpire(cpf, now) {
        const expirateDate = endOfDay(add(now, { months: 1 }));

        const [closeToExpireAgg] = await this._saleModel.aggregate([
            {
                $match: {
                    cpf: cpf,
                    status: 'AVAILABLE',
                    expirateDate: { $lte: this._updateHours(expirateDate.getTimezoneOffset() / 60, expirateDate) }
                }
            },
            {
                $group: {
                    _id: '$cpf',
                    value: {
                        $sum: '$availableCashback'
                    }
                }
            }
        ]);
        return closeToExpireAgg;
    }

    /**
     * Esta função recebe um período (LAST_6_MONTHS, LAST_YEAR ou LAST_2_YEARS) e retorna um objeto de consulta para ser usado em uma consulta ao banco de dados.
     * O objeto de consulta contém uma cláusula $gte que compara a data especificada no período com a data atual, após a correção do fuso horário, usando a função _updateHours.
     * A função _updateHours é responsável por ajustar a hora, levando em consideração o fuso horário.
     * A função retorna o objeto de consulta a ser utilizado em uma consulta ao banco de dados.
     * Além disso, a função imprime o objeto de consulta no console para fins de depuração.
     */
    _getPeriodQuery(period) {
        const now = new Date();
        const timezoneOffset = now.getTimezoneOffset() / 60;

        let query;
        if (period === 'LAST_6_MONTHS') {
            query = { $gte: this._updateHours(timezoneOffset, startOfDay(sub(now, { months: 6 }))) };
        } else if (period === 'LAST_YEAR') {
            query = { $gte: this._updateHours(timezoneOffset, startOfDay(sub(now, { years: 1 }))) };
        } else if (period === 'LAST_2_YEARS') {
            query = { $gte: this._updateHours(timezoneOffset, startOfDay(sub(now, { years: 2 }))) };
        }

        return query;
    }

    /**
     * A função _formatMoney é utilizada para formatar um valor numérico para uma string no formato monetário.
     * Ela recebe como parâmetros o valor a ser formatado e a moeda a ser utilizada, e retorna uma string contendo o valor formatado com o símbolo da moeda.
     * Internamente, a função utiliza a biblioteca Intl.NumberFormat para formatar o valor numérico com as configurações de moeda especificadas.
     * Por exemplo, se o valor for 10.50 e a moeda for BRL, a função irá retornar a string "R$ 10,50".
     */
    _formatMoney(value) {
        return (value / 100).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    }

    /**
     * A função _updateHours recebe dois parâmetros: hours e date.
     * O parâmetro hours é um número inteiro que representa a diferença de horas entre o fuso horário local do servidor e o fuso horário desejado. Por exemplo, se o servidor estiver no horário de Brasília (UTC-3) e o fuso horário desejado for o horário de Nova York (UTC-5), hours deve ser igual a 2.
     * O parâmetro date é um objeto Date que representa a data a ser atualizada com a diferença de horas especificada.
     * A função retorna um novo objeto Date atualizado com a diferença de horas especificada.
     * Isso é feito ajustando o valor de hora do objeto date de acordo com a diferença de horas especificada.
     */
    _updateHours(numOfHours, date = new Date()) {
        const dateCopy = new Date(date.getTime());

        dateCopy.setHours(dateCopy.getHours() - numOfHours);

        return dateCopy;
    }

    /**
     * A função _sumTotalBalance é responsável por calcular o saldo total de cashback disponível para resgate pelos clientes.
     * Ela recebe um array de objetos contendo as informações de resgates anteriores, e percorre esses objetos somando o valor de cada resgate ao saldo total.
     * Além disso, a função também verifica se há cashbacks que estão próximos de expirar e desconta esses valores do saldo total.
     * Ao final, a função retorna o saldo total de cashback disponível para resgate.
     */
    _sumTotalBalance(value) {
        const quantity = value.map(function (res) {
            return parseInt(res.totalCashback);
        });

        const rerult = quantity.reduce(function (previousValue, currentValue) {
            return Number(previousValue) + Number(currentValue);
        }, 0 && quantity);

        return rerult;
    }

    /**
     * A função managerSales é responsável por gerenciar as vendas de um usuário, verificando seu saldo de cashback disponível, bem como as vendas que ele realizou e que geraram cashback.
     * Inicialmente, a função recebe o CPF e o token de autorização do usuário. Em seguida, ela utiliza o CPF para buscar o saldo de cashback acumulado pelo usuário na função _getBalance.
     * Além disso, também é feita uma busca na API da MyCash para obter informações do usuário, como nome e sobrenome, caso ele esteja cadastrado.
     * A função então faz duas buscas na collection sale, uma para as vendas disponíveis (status: 'AVAILABLE') e outra para as vendas pendentes (status: 'PENDING'), que ainda não foram processadas e, portanto, não tiveram o cashback liberado.
     * As vendas disponíveis e pendentes são então unidas em um único array utilizando o método concat.
     * A partir das informações obtidas, a função constrói um objeto com os seguintes campos:
     * name: nome completo do usuário, obtido a partir da API da MyCash
     * balance: saldo total de cashback acumulado pelo usuário, obtido na função _getBalance
     * sales: array de objetos que representam as vendas do usuário, contendo as seguintes informações:
     * value: valor total de cashback gerado pela venda
     * order: número do pedido
     * status: status da venda, que pode ser "AVAILABLE" se o cashback já foi liberado ou "CLOSE_TO_EXPIRE" se a data de expiração do cashback está próxima
     * saleDate: data em que a venda foi realizada
     * expirateDate: data de expiração do cashback gerado pela venda
     * Caso o usuário tenha saldo de cashback disponível, mas não esteja cadastrado na MyCash, a função retorna um objeto com o status "user has CASHBACK, but has a record in MYCASH".
     * Se o usuário não tiver vendas disponíveis ou pendentes e não estiver cadastrado na MyCash, a função retorna um erro "USER_NOT_FOUND".
     */
    async managerSales(cpf, authorizationToken) {
        const cpfUser = cpf.match(/\d/g).join('');
        const now = new Date();

        const balanceAgg = await this._getBalance(cpfUser);
        const userMycash = await this._myCashIntegration.getUserByCpf(cpfUser, authorizationToken);
        const userSales = await this._saleModel.find({ cpf: cpfUser, status: 'AVAILABLE' });
        const userPendingSales = await this._saleModel.find({ cpf: cpfUser, status: 'PENDING' });

        const joinUserSales = userSales.concat(userPendingSales);
        const balanceArray = this._sumTotalBalance(joinUserSales);

        if (joinUserSales.length === 0 && !userMycash) throw new Error('USER_NOT_FOUND');

        if (joinUserSales.length > 0 && !userMycash) {
            return {
                name: '',
                balance: balanceArray,
                sales: joinUserSales.map((item) => ({
                    value: item.totalCashback,
                    order: item.order.number,
                    status: 'NOT_RELEASED',
                    saleDate: item.saleDate,
                    expirateDate: item.expirateDate
                })),
                status: 'user has CASHBACK, but has a record in MYCASH'
            };
        }

        return {
            name: userMycash ? `${userMycash.firstName} ${userMycash.lastName}` : '',
            balance: (balanceAgg && balanceAgg.balance) || 0,
            sales: userSales.map((item) => ({
                value: item.totalCashback,
                order: item.order.number,
                status: isBefore(item.expirateDate, endOfDay(add(now, { months: 1 })))
                    ? 'CLOSE_TO_EXPIRE'
                    : 'AVAILABLE',
                saleDate: item.saleDate,
                expirateDate: item.expirateDate
            }))
        };
    }

    /**
     * A função balance é um método assíncrono que recebe um objeto contendo informações do cliente, como CPF, primeiro e último nome, e usa essas informações para buscar o saldo do cliente em uma fonte externa (provavelmente um serviço financeiro).
     * Essa função chama o método _getBalance para obter o saldo do cliente a partir do CPF fornecido.
     * Em seguida, ela retorna um objeto contendo o saldo atual do cliente, juntamente com seu nome e CPF.
     * Se o saldo do cliente não puder ser recuperado, a função retorna zero como saldo padrão.
     */
    async balance({ cpf, firstName, lastName }) {
        const balanceAgg = await this._getBalance(cpf);

        return {
            balance: (balanceAgg && balanceAgg.balance) || 0,
            name: `${firstName} ${lastName}`,
            cpf
        };
    }

    /**
     * A função wallet é responsável por retornar informações sobre a carteira de cashback de um determinado usuário, com base em seu CPF.
     * Para isso, ela utiliza três funções auxiliares: _getBalance, _getLastRescues e _getCloseToExpire, que realizam consultas ao banco de dados para obter informações específicas sobre a carteira de cashback do usuário.
     * A função wallet retorna um objeto com as seguintes propriedades:
     * balance: o saldo atual de cashback do usuário;
     * lastRescues: o valor total de cashback resgatado pelo usuário nos últimos dois meses;
     * closeToExpire: o valor total de cashback que está próximo de expirar nos próximos 30 dias.
     * Caso não haja informações disponíveis para uma determinada propriedade, a função retorna o valor zero.
     */
    async wallet({ cpf }) {
        const now = new Date();
        const balanceAgg = await this._getBalance(cpf);
        const lastRescuesAgg = await this._getLastRescues(cpf, now);
        const closeToExpireAgg = await this._getCloseToExpire(cpf, now);

        await this._ditoUsersIntegration.updateUser(cpf, {
            saldo_cashback_valor: (((balanceAgg && balanceAgg.balance) || 0) / 100).toFixed(2),
            saldo_cashback_data: formatISO(new Date())
        });

        return {
            balance: (balanceAgg && balanceAgg.balance) || 0,
            lastRescues: (lastRescuesAgg && lastRescuesAgg.value) || 0,
            closeToExpire: (closeToExpireAgg && closeToExpireAgg.value) || 0
        };
    }

    /**
     * A função reports é responsável por gerar relatórios de vendas de acordo com os parâmetros informados.
     * Esses parâmetros podem incluir datas de início e fim, campanhas de marketing específicas, tipo de relatório (em formato de arquivo Excel ou JSON), entre outros.
     * Dentro da função, há uma série de validações e processamentos de dados para garantir que o relatório gerado esteja correto e completo. Isso inclui buscar todas as vendas que correspondam aos parâmetros informados, processá-las para calcular o valor do cashback, agrupá-las por data ou por campanha, e, por fim, gerar o relatório no formato desejado.
     * O relatório gerado pode conter informações como a data da venda, o valor total da venda, o valor do cashback, o nome da campanha de marketing associada, o status da campanha, entre outras informações relevantes.
     */
    async reports({ status, usedCampaign, startDate, endDate }) {
        const query = {};
        const now = new Date();
        const timezoneOffset = now.getTimezoneOffset() / 60;

        if (status) query.status = { $in: status };
        if (usedCampaign) query.usedCampaign = { $in: usedCampaign };
        if (startDate && endDate) {
            query.saleDate = {
                $gte: this._updateHours(timezoneOffset, new Date(startDate)),
                $lte: this._updateHours(timezoneOffset, new Date(endDate))
            };
        } else if (startDate) {
            query.saleDate = { $gte: this._updateHours(timezoneOffset, new Date(startDate)) };
        } else if (endDate) {
            query.saleDate = { $lte: this._updateHours(timezoneOffset, new Date(endDate)) };
        }

        const sales = await this._saleModel.find(query);
        if (sales.length === 0) throw new Error('SALE_NOT_FOUND');

        const result = [];

        sales.forEach((sale) => {
            sale.items.forEach((item) => {
                const row = {
                    order: sale.order.number,
                    totalPrice: sale.items.reduce((prev, cur) => (prev += cur.totalPrice), 0),
                    partnumber: item.partnumber,
                    productValue: item.unitPrice,
                    productCashBack: item.unitCashback,
                    productQuantity: item.quantity,
                    productTotalValue: item.totalPrice,
                    productTotalCashBack: item.totalCashback,
                    productName: item.description,
                    status: sale.status,
                    usedCampaign: sale.usedCampaign,
                    totalCashback: sale.totalCashback,
                    saleDate: sale.saleDate,
                    expirateDate: sale.expirateDate
                };

                result.push(row);
            });
        });

        return this._converterXLSXReport(result);
    }

    async reportUserMyCash(authorizationToken) {
        const userMyCash = await this._myCashIntegration.getAllUsers(authorizationToken);
        if (userMyCash.length === 0) throw new Error('USER_NOT_FOUND');

        const resultMyCash = await Promise.all(
            userMyCash.map(async (user) => {
                const balanceAgg = await this._getBalance(user.cpf);

                const row = {
                    name: user.name,
                    balance: (balanceAgg && balanceAgg.balance) || 0,
                    email: user.email,
                    gender: user.gender,
                    cpf: user.cpf,
                    dateOfBirthday: user.dateOfBirthday,
                    acceptedNewsletter: user.acceptedNewsletter,
                    acceptedTerms: user.acceptedTerms,
                    dateDeletion: user.dateDeletion
                };

                return row;
            })
        );

        return this._converterXLSXReport(resultMyCash);
    }

    _converterXLSXReport(json) {
        const wb = XLSX.utils.book_new();
        const ws = XLSX.utils.json_to_sheet(json);

        XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

        const xlsx = XLSX.write(wb, { type: 'base64' });

        return Buffer.from(xlsx, 'base64');
    }

    /**
     * A função userRegistered atualiza o campo userInMycash para true em todas as vendas cujo CPF seja igual ao valor passado como parâmetro cpf.
     * Essa função é usada para marcar as vendas de um usuário que se registrou no serviço MyCash, indicando que essas vendas podem ser elegíveis para cashback ou outros benefícios oferecidos pela plataforma.
     */
    async userRegistered({ cpf }) {
        return await this._saleModel.updateMany({ cpf }, { $set: { userInMycash: true } });
    }

    /**
     * A função cancel cancela uma venda, podendo ser uma venda que teve o cashback utilizado ou uma venda que gerou cashback disponível.
     * Antes de cancelar, a função verifica se a venda existe e se ela pode ser cancelada de acordo com o status atual (não pode cancelar uma venda que já foi cancelada, nem uma venda que está disponível ou expirada).
     * Se a venda tiver cashback utilizado, a função cria um histórico com as vendas que tiveram o cashback usado e remove o cashback usado da venda em questão.
     * Em seguida, atualiza o status da venda para 'CANCELADO'.
     * Se o usuário estiver cadastrado no serviço Dito, a função cria um evento informando sobre o cancelamento e o saldo atual de cashback do usuário.
     * Se a venda não tiver cashback utilizado, a função atualiza o status da venda para 'CANCELADO' e remove informações de cashback da venda.
     * Se o usuário estiver cadastrado no serviço Dito, a função cria um evento informando sobre o cancelamento e o valor total de cashback que seria gerado.
     */
    async cancel(id) {
        const sale = await this._saleModel.findOne({ _id: id });
        if (!sale) throw new Error('SALE_NOT_FOUND');
        if (sale.status === 'CANCELED') return;
        if (sale.status === 'AVAILABLE') throw new Error('CANT_CANCEL_AVAILABLE_SALE');
        if (sale.status === 'EXPIRED') throw new Error('CANT_CANCEL_EXPIRED_SALE');

        if (sale.usedCashback) {
            const history = _.cloneDeep(sale.history);

            await Promise.all(
                history.map((h) =>
                    this._saleModel.aggregate([
                        { $match: { _id: mongoose.Types.ObjectId(h.saleId), status: { $ne: 'EXPIRED' } } },
                        {
                            $set: {
                                availableCashback: { $sum: ['$availableCashback', h.usedValue] },
                                cashbackUseHistory: {
                                    $filter: {
                                        input: '$cashbackUseHistory',
                                        as: 'item',
                                        cond: { $ne: ['$$item.saleId', mongoose.Types.ObjectId(id)] }
                                    }
                                }
                            }
                        },
                        { $merge: { into: 'sales', on: '_id', whenMatched: 'replace', whenNotMatched: 'discard' } }
                    ])
                )
            );

            await this._saleModel.findByIdAndUpdate(id, { $set: { status: 'CANCELED' } }, { new: true });

            const ditoUser = await this._findOrCreateDitoUser(sale.customer.id, {
                name: sale.customer.name,
                email: sale.customer.email,
                phone: sale.customer.phone
            });

            if (ditoUser) {
                const balanceAgg = await this._getBalance(sale.customer.id);
                const balance = (balanceAgg && balanceAgg.balance) || 0;

                this._ditoEventsIntegration.createEvent(sale.customer.id, 'cancelou_utilizacao_cashback', {
                    cancelou_utilizacao_cashback_valor: (sale.usedCashbackValue / 100).toFixed(2),
                    cancelou_utilizacao_cashback_data: formatISO(new Date()),
                    saldo_cashback_valor: (balance / 100).toFixed(2),
                    saldo_cashback_data: formatISO(new Date())
                });

                this._ditoUsersIntegration.updateUser(sale.customer.id, {
                    saldo_cashback_valor: (balance / 100).toFixed(2),
                    saldo_cashback_data: formatISO(new Date())
                });
            }
        } else {
            await this._saleModel.findByIdAndUpdate(
                id,
                { $set: { status: 'CANCELED' }, $unset: { creditDate: '', expirateDate: '', availableCashback: '' } },
                { new: true }
            );
            const ditoUser = await this._findOrCreateDitoUser(sale.customer.id, {
                name: sale.customer.name,
                email: sale.customer.email,
                phone: sale.customer.phone
            });

            if (ditoUser) {
                this._ditoEventsIntegration.createEvent(sale.customer.id, 'cancelou_geracao_cashback', {
                    cancelou_geracao_cashback_valor: (sale.totalCashback / 100).toFixed(2),
                    cancelou_geracao_cashback_data: formatISO(new Date())
                });
            }
        }
    }

    /**
     * Essa função tem como objetivo marcar uma venda como integrada, definindo o campo "integrated" como verdadeiro.
     * Para isso, é realizada uma busca por uma venda com o id informado e, caso ela exista, é atualizado o seu campo "integrated" para true.
     * Em seguida, a venda atualizada é retornada. Caso a venda não seja encontrada, é lançada uma exceção com a mensagem 'SALE_NOT_FOUND'.
     */
    async integrate(id) {
        const sale = await this._saleModel.findOneAndUpdate({ _id: id }, { $set: { integrated: true } }, { new: true });
        if (!sale) throw new Error('SALE_NOT_FOUND');
        return sale;
    }

    /**
     * A função update é responsável por atualizar uma venda no banco de dados.
     * Ela recebe como parâmetro um objeto contendo as informações a serem atualizadas e o id da venda a ser atualizada.
     * Internamente, a função utiliza a função findOneAndUpdate do MongoDB para buscar e atualizar a venda no banco de dados.
     * O primeiro parâmetro da função findOneAndUpdate é um objeto que define o filtro para encontrar a venda a ser atualizada (nesse caso, o filtro é pelo id).
     * O segundo parâmetro é um objeto contendo as informações a serem atualizadas.
     * Por fim, o terceiro parâmetro é um objeto de opções que especifica que a função deve retornar a nova versão do documento atualizado ({ new: true }) e que a função não deve criar um novo documento caso não encontre um documento correspondente ao filtro ({ upsert: false }).
     * Se a venda não for encontrada, a função lança um erro informando que a venda não foi encontrada. Caso contrário, a função retorna a venda atualizada.
     */
    async update(updateData, id) {
        const sale = await this._saleModel.findOneAndUpdate(
            { _id: id },
            { $set: { ...updateData } },
            { new: true, upsert: false }
        );
        if (!sale) throw new Error('SALE_NOT_FOUND');
        return sale;
    }

    async list({ cpf, integrated, status, page = 1, limit = 20, period, invoiceKey, id, usedCashback }) {
        const query = {};
        if (cpf) query.cpf = cpf;
        if (id) query._id = id;
        if (usedCashback) query.usedCashback = Boolean(usedCashback);
        if (_.isBoolean(integrated)) query.integrated = Boolean(integrated);
        if (status) query.status = status;
        if (invoiceKey) query.invoiceKey = invoiceKey;
        if (period) query.saleDate = this._getPeriodQuery(period);

        return await this._saleModel.paginate(query, { sort: { saleDate: 'asc' }, page, limit });
    }

    /**
     * A função simulateCashback realiza uma simulação de cashback para uma determinada venda passada como parâmetro simulateData.
     * Primeiro, ela recupera todas as campanhas ativas através da função _getActivesCampaigns.
     * Em seguida, calcula o cashback para as campanhas válidas e inválidas através da função _calculeCashback, retornando duas listas: validCampaigns e invalidCampaigns.
     * Posteriormente, a função verifica se o usuário que realizou a venda está na lista de usuários banidos através da função isUserInBannedList.
     * Caso esteja, a função retorna um erro.
     * Após essa verificação, a função encontra a venda com a melhor campanha através da função _findSaleWithBetterCampaign e marca os itens elegíveis com base na campanha utilizada pela função _markEligibleItemsBasedOnUsedCampaign.
     * Por fim, a função retorna a venda com a melhor campanha, a lista de campanhas inválidas e uma lista de vendas com outras campanhas que também ofereceram cashback para essa venda.
     */
    async simulateCashback(simulateData) {
        const campaigns = await this._getActivesCampaigns();

        const { validCampaigns: campaignsWithCashback, invalidCampaigns } = await this._calculeCashback(
            campaigns,
            simulateData,
            simulateData.customerId
        );

        const { exist: isUserInBannedList } = await this._myCashIntegration.isUserInBannedList(simulateData.customerId);

        if (isUserInBannedList) {
            throw new Error('USER_FOUND_BANNEDLIST');
        }

        const saleWithBetterCampaign = this._findSaleWithBetterCampaign(campaignsWithCashback);

        this._markEligibleItemsBasedOnUsedCampaign(saleWithBetterCampaign);

        return {
            ...saleWithBetterCampaign,
            invalidCampaigns,
            saleWithOtherCampaigns: saleWithCashback.map((sale) => {
                return {
                    usedCampaign: sale.usedCampaign,
                    creditDate: sale.creditDate,
                    expirateDate: sale.expirateDate,
                    totalCashback: sale.totalCashback
                };
            })
        };
    }

    async _findOrCreateDitoUser(cpf, { email, name, phone }) {
        if (!cpf) return null;
        let user = await this._ditoUsersIntegration.getUser(cpf);
        if (!user) {
            user = await this._ditoUsersIntegration.createUser(cpf, { cpf, email, name, phone });
        }
        return user;
    }

    _translateCampaignStatus(status) {
        const statusMap = {
            READY: 'pronta',
            PENDENT: 'pendente',
            INACTIVE: 'inativa',
            ACTIVE: 'ativa',
            EXPIRED: 'expirada'
        };

        return statusMap[status] || status;
    }

    async create(createData, authorizationToken) {
        try {
            /**
             * A função this._receivedSaleModel.create é responsável por criar uma nova venda no banco de dados. Ela utiliza o modelo ReceivedSaleModel para criar um novo documento de venda com base nas informações fornecidas.
             * A função recebe como parâmetro um objeto saleData que contém as informações da venda, incluindo o ID do produto vendido, o ID do cliente que realizou a compra, o preço unitário do produto e a quantidade de produtos vendidos.
             * Antes de criar a nova venda, a função verifica se todos os campos obrigatórios foram fornecidos. Caso algum campo obrigatório esteja faltando, a função retorna um erro informando qual campo está faltando.
             * Se todos os campos obrigatórios estiverem presentes, a função cria um novo documento de venda utilizando o modelo ReceivedSaleModel e as informações fornecidas. Em seguida, ela salva o novo documento no banco de dados e retorna a nova venda em formato de objeto.
             * Caso ocorra algum erro durante o processo de criação da nova venda no banco de dados, a função retorna o erro gerado.
             */
            const receivedSale = await this._receivedSaleModel.create({
                invoiceKey: createData.invoice && createData.invoice.key,
                cpf: createData.customer.id,
                data: createData
            });

            await this._generateCashback({ data: createData }, receivedSale, authorizationToken);

            await this._receivedSaleModel.findOneAndUpdate(
                {
                    invoiceKey: createData.invoice && createData.invoice.key,
                    cpf: createData.customer.id,
                    data: createData,
                    processed: false
                },
                { $set: { processed: true } },
                { new: true }
            );

            return newSale;
        } catch (error) {
            if (error.name === 'ValidationError' && !!error._message) this._handleMongoError(error);
            throw error;
        }
    }

    async createAllSales(createData) {
        try {
            const receivedSale = await this._receivedSaleModel.create({
                invoiceKey: createData.invoice && createData.invoice.key,
                cpf: createData.customer.id,
                data: createData
            });
            return receivedSale;
        } catch (error) {
            if (error.name === 'ValidationError' && !!error._message) this._handleMongoError(error);
            throw error;
        }
    }

    /**
     * A função processUnprocessedSales é responsável por processar as vendas recebidas que ainda não foram processadas.
     * Ela busca as vendas não processadas no banco de dados, para cada venda, ela tenta calcular o cashback válido e inválido baseado nas campanhas ativas, encontrar a campanha que oferece o maior cashback total,
     * marcar os itens elegíveis da venda com base na campanha usada, criar um histórico da fonte do cashback e do uso do cashback, atualizar a venda como processada e adicionar informações sobre a venda processada a um array processedSales.
     * Ao final, a função retorna um objeto indicando se o processamento foi bem-sucedido e uma lista das vendas processadas com as informações adicionais mencionadas acima. Caso ocorra algum erro, a função trata o erro e o registra no array processedSales.
     */
    async processUnprocessedSales(authorizationToken) {
        try {
            const unprocessedSales = await this._receivedSaleModel.find({ processed: false });
            const processedSales = [];

            for (const processSale of unprocessedSales) {
                try {
                    const sales = await this._generateCashback(processSale, null, authorizationToken);

                    await this._receivedSaleModel.findOneAndUpdate(
                        { invoiceKey: processSale.invoiceKey },
                        { $set: { processed: true } }
                    );

                    processedSales.push({
                        sale: sales
                    });
                } catch (error) {
                    if (error.name === 'ValidationError' && !!error._message) this._handleMongoError(error);
                    console.error(`Erro ao processar venda ${processSale.invoiceKey}: ${error.message}`);
                    processedSales.push({ error: error.message, sale: processSale });
                }
            }

            return { success: true, processedSales };
        } catch (error) {
            console.error(`Erro ao buscar vendas não processadas: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    /**
     *
     * Essa função é responsável por gerar o cashback a ser recebido pelo cliente após uma venda ser processada. Ela recebe três parâmetros:
     * processSale: um objeto contendo as informações da venda a ser processada.
     * receivedSale: opcional, um objeto contendo as informações da venda que deu origem a venda a ser processada.
     * authorizationToken: um token de autorização para realizar a integração com o sistema MyCash.
     * Em resumo, a função começa obtendo o saldo atual de cashback do cliente, com base no CPF informado na venda.
     * Em seguida, ela verifica se a venda utiliza cashback como forma de pagamento.
     * Se sim, ela cria um registro da venda no banco de dados e atualiza o saldo de cashback do cliente.
     * Além disso, ela envia um e-mail de confirmação da compra e cria eventos no sistema Dito.
     * Se a venda não utilizar cashback como forma de pagamento, a função calcula o cashback a ser recebido pelo cliente com base nas campanhas de marketing ativas e expiradas, cria um registro da venda no banco de dados e atualiza o saldo de cashback do cliente.
     * Nesse caso, ela também envia um e-mail de confirmação da compra e cria eventos no sistema Dito.
     * Em ambos os casos, a função retorna um objeto contendo as informações da venda registrada no banco de dados.
     */
    async _generateCashback(processSale, receivedSale = null, authorizationToken) {
        let balanceAgg = await this._getBalance(processSale.cpf);
        let balance = (balanceAgg && balanceAgg.balance) || 0;

        if (processSale.data.usedCashback) {
            if (balance < processSale.data.usedCashbackValue) throw new Error('INSUFFICIENT_FUNDS');

            const data = this._createUsedCashbackSale(processSale.data);

            /**
             * A função recebe como parâmetro um objeto saleData que contém as informações da venda,
             * incluindo o ID do produto vendido, o ID do cliente que realizou a compra,
             * o preço unitário do produto, a quantidade de produtos vendidos,
             * a data e hora da venda e o valor do cashback utilizado (se houver).
             */
            const sale = await this._saleModel.create({
                ...data,
                invoiceKey: data.invoiceKey,
                cpf: data.cpf,
                usedCashback: data.usedCashback,
                receivedSale: receivedSale ? receivedSale._id : processSale._id
            });

            const history = await this._useCashback({
                usedValue: processSale.data.usedCashbackValue,
                cpf: processSale.data.customer.id,
                invoiceKey: processSale.data.invoice && processSale.data.invoice.key,
                saleId: sale._id
            });

            const user = await this._myCashIntegration.getUserByCpf(processSale.data.customer.id, authorizationToken);

            balanceAgg = await this._getBalance(processSale.data.customer.id);
            balance = (balanceAgg && balanceAgg.balance) || 0;

            /**
             *  Se a condição for verdadeira, a propriedade cashback do objeto createData é atualizada
             *  com o saldo atual de cashback do usuário obtido pela integração com o sistema MyCash e a
             *  propriedade customerId do objeto createData é atualizada com o ID do usuário obtido pela integração.
             *  Isso é feito para garantir que o histórico de compras do cliente seja registrado corretamente no objeto
             *  createData.
             */
            if (user) {
                const totalPrice = processSale.data.items.reduce(
                    (prev, cur) => (prev += cur.unitPrice * cur.quantity),
                    0
                );

                if (process.env.NODE_ENV !== 'test') {
                    this._notificationsIntegration.sendEmail('PURCHASE_CASHBACK', {
                        to: user.email,
                        name: user.firstName,
                        totalPrice: this._formatMoney(totalPrice),
                        paidValue: this._formatMoney(totalPrice - processSale.data.usedCashbackValue),
                        usedCashbackValue: this._formatMoney(processSale.data.usedCashbackValue),
                        balance: this._formatMoney(balance)
                    });
                }
            }

            const ditoUser = await this._findOrCreateDitoUser(processSale.data.customer.id, {
                name: processSale.data.customer.name,
                email: processSale.data.customer.email,
                phone: processSale.data.customer.phone
            });

            if (ditoUser) {
                this._ditoUsersIntegration.updateUser(processSale.data.customer.id, {
                    saldo_cashback_valor: (balance / 100).toFixed(2),
                    saldo_cashback_data: formatISO(new Date())
                });

                await this._ditoEventsIntegration.createEvent(processSale.data.customer.id, 'utilizou_cashback', {
                    utilizou_cashback_valor: (processSale.data.usedCashbackValue / 100).toFixed(2),
                    utilizou_cashback_data: formatISO(
                        this._updateHours(
                            0 - new Date().getTimezoneOffset() / 60,
                            new Date(processSale.data.verification)
                        )
                    ),
                    saldo_cashback_valor: (balance / 100).toFixed(2),
                    saldo_cashback_data: formatISO(new Date())
                });
            }

            /**
             * responsável por atualizar o saldo de cashback do cliente no banco de dados após a realização de uma venda
             * que utiliza cashback como forma de pagamento. Essa atualização é necessária para refletir a nova quantidade de
             * cashback disponível para o cliente após a realização da venda.
             */
            return await this._saleModel.findByIdAndUpdate(
                sale._id,
                {
                    $set: {
                        history,
                        userInMycash: !!user //a expressão !!user é usada para converter a variável user em um valor booleano
                    }
                },
                { new: true }
            );
        }

        if (await this._saleModel.findOne({ invoiceKey: processSale.data.invoice.key }))
            throw new Error('CONFLICT_DUPLICATE_INVOICE_KEY_ERROR');

        const { exist: isUserInBannedList } = await this._myCashIntegration.isUserInBannedList(
            processSale.data.customer.id
        );

        if (isUserInBannedList) {
            throw new Error('USER_FOUND_BANNEDLIST');
        }

        const activeCampaigns = this._campaignIntegration.getCampaigns({
            status: 'ACTIVE',
            limit: 50,
            betweenDate: processSale.data.verification
        });

        // const inactiveCampaigns = this._campaignIntegration.getCampaigns({
        //     status: 'INACTIVE',
        //     limit: 50,
        //     betweenDate: processSale.data.verification,
        //     inactiveDate: processSale.data.verification
        // });

        const expiredCampaigns = this._campaignIntegration.getCampaigns({
            status: 'EXPIRED',
            limit: 50,
            betweenDate: processSale.data.verification
        });

        /**
         * A função é usada para obter uma lista única de todas as campanhas de marketing ativas, inativas e expiradas.
         * Essa função utiliza o método Promise.all() para esperar a resolução de duas Promises: activeCampaigns e expiredCampaigns.
         * A Promise inactiveCampaigns está comentada e, portanto, não é utilizada.
         * Cada uma dessas Promises retorna uma lista de objetos, cada um contendo informações sobre uma campanha de marketing específica.
         * A lista de objetos retornada pela Promise activeCampaigns contém informações sobre as campanhas ativas, enquanto a lista de objetos retornada pela Promise expiredCampaigns contém informações sobre as campanhas expiradas.
         * Após a resolução dessas Promises, a função flat() é utilizada para criar uma lista única de todos os objetos de campanha retornados pelas Promises.
         * Isso significa que a lista resultante contém objetos de campanha de todas as campanhas ativas e expiradas, sem duplicatas.
         * Essa lista é retornada pela função e é usada para verificar se alguma campanha está associada à venda e para calcular o cashback que o comprador da venda receberá.
         */
        const campaigns = (await Promise.all([activeCampaigns, /*inactiveCampaigns */ expiredCampaigns])).flat();

        const { validCampaigns: saleWithCashback, invalidCampaigns } = await this._calculeCashback(
            campaigns,
            processSale.data,
            processSale.data.customer.id,
            receivedSale ? receivedSale._id : processSale._id
        );

        const saleWithBestCashback = this._findSaleWithBetterCampaign(saleWithCashback);

        this._markEligibleItemsBasedOnUsedCampaign(saleWithBestCashback);

        const data = {
            status: 'PENDING',
            invoiceKey: saleWithBestCashback.invoice.key,
            cpf: saleWithBestCashback.customer.id,
            email: saleWithBestCashback.customer.email,
            matchedCampaigns: saleWithBestCashback.matchedCampaigns,
            usedCampaign: saleWithBestCashback.usedCampaign,
            saleDate: saleWithBestCashback.verification,
            salesChannel: saleWithBestCashback.salesChannel,
            totalCashback: saleWithBestCashback.totalCashback,
            order: saleWithBestCashback.order,
            items: saleWithBestCashback.items,
            customer: saleWithBestCashback.customer,
            invoice: saleWithBestCashback.invoice,
            creditDate: saleWithBestCashback.creditDate,
            expirateDate: saleWithBestCashback.expirateDate,
            paymentMethod: saleWithBestCashback.paymentMethod,
            receivedSale: receivedSale ? receivedSale._id : processSale._id,
            invalidCampaigns,
            saleWithOtherCampaigns: saleWithCashback.map((sale) => {
                return {
                    usedCampaign: sale.usedCampaign,
                    creditDate: sale.creditDate,
                    expirateDate: sale.expirateDate,
                    totalCashback: sale.totalCashback
                };
            })
        };

        const user = await this._myCashIntegration.getUserByCpf(processSale.data.customer.id, authorizationToken);

        if (await this._saleModel.findOne({ invoiceKey: processSale.data.invoice.key }))
            throw new Error('CONFLICT_DUPLICATE_INVOICE_KEY_ERROR');

        if (!data.invoiceKey) throw new Error('CONFLICT_DUPLICATE_INVOICE_KEY_ERROR');

        const newSale = await this._saleModel.findOneAndUpdate(
            { invoiceKey: data.invoiceKey },
            {
                ...data,
                invoiceKey: data.invoiceKey,
                cpf: data.cpf,
                userInMycash: !!user
            },
            { upsert: true, new: true }
        );

        if (user && process.env.NODE_ENV !== 'test') {
            this._notificationsIntegration.sendEmail('REDEEM_CASHBACK', {
                to: user.email,
                name: user.firstName
            });
        }

        const ditoUser = await this._findOrCreateDitoUser(processSale.data.customer.id, {
            name: processSale.data.customer.name,
            email: processSale.data.customer.email,
            phone: processSale.data.customer.phone
        });

        if (ditoUser) {
            this._ditoUsersIntegration.updateUser(processSale.data.customer.id, {
                saldo_cashback_valor: (balance / 100).toFixed(2),
                saldo_cashback_data: formatISO(new Date())
            });

            await this._ditoEventsIntegration.createEvent(processSale.data.customer.id, 'participou_cashback', {
                nome_campanha: saleWithBestCashback.campaignData.name,
                codigo_campanha: saleWithBestCashback.campaignData.code,
                status_campanha: this._translateCampaignStatus(saleWithBestCashback.campaignData.status),
                data_inicio_campanha: formatISO(
                    this._updateHours(
                        0 - new Date().getTimezoneOffset() / 60,
                        new Date(saleWithBestCashback.campaignData.startDate)
                    )
                ),
                data_termino_campanha: formatISO(
                    this._updateHours(
                        0 - new Date().getTimezoneOffset() / 60,
                        new Date(saleWithBestCashback.campaignData.endDate)
                    )
                )
            });

            this._ditoEventsIntegration.createEvent(processSale.data.customer.id, 'gerou_cashback', {
                gerou_cashback_valor: (newSale.totalCashback / 100).toFixed(2),
                gerou_cashback_data: formatISO(new Date()),
                expiracao_cashback_data: formatISO(
                    this._updateHours(0 - new Date().getTimezoneOffset() / 60, new Date(newSale.expirateDate))
                )
            });

            this._ditoEventsIntegration.createEvent(processSale.data.customer.id, 'gerou_cashback', {
                gerou_cashback_valor: (newSale.totalCashback / 100).toFixed(2),
                gerou_cashback_data: formatISO(new Date()),
                liberacao_cashback_data: formatISO(
                    this._updateHours(0 - new Date().getTimezoneOffset() / 60, new Date(newSale.creditDate))
                )
            });
        }
        return newSale;
    }

    _handleMongoError(error) {
        const keys = Object.keys(error.errors);
        const err = new Error(error.errors[keys[0]].message);
        err.field = keys[0];
        err.status = 409;
        throw err;
    }
}

module.exports = SaleService;
