rm(list = ls())
gc()
# Fluxos PDAD
install.packages('sampleSelection')
# Carregar pacotes
library(odbc)
library(tidyverse)
library(survey)
library(srvyr)
library(DBI)
library(lubridate)
library(sidrar)
library(sampleSelection)
library(dplyr)

# Abrir conexão com o banco de dados
db<- DBI::dbConnect(odbc::odbc(),"db_codeplan",
                    uid=("104409"),
                    pwd=("u5rcdp1l4n"))

# Baixar base de moradores
pdad_mor_2018 <- DBI::dbGetQuery(db,"select * from pdad2018.moradores_final_pesos_imputada") 

# Baixar base de domicílios
pdad_dom_2018 <- DBI::dbGetQuery(db,"select * from pdad2018.domicilios_final_pesos_imputada") 

#verificando em que mes as visitas comecaram 
datav<-mdy(pdad_2018$datavisita)
data <-format(datav, "%d-%m-%Y")
a<-data.frame(data) %>% mutate(mes=month(data))
a<-a[order(a$mes,decreasing = F),]


##DUVIDA#########
# Juntar as bases em um arquivo
pdad_2018<-left_join(pdad_dom_2018,
                     pdad_mor_2018 %>% 
                       dplyr::select(-c(A01ra,FATOR_PROJ,datavisita,horavisita,infladefla))) %>% 
  dplyr::mutate(neto=case_when(E02==10~1,
                               TRUE~0),
                bisneto=case_when(E02==11~1,
                                  TRUE~0),
                sogro=case_when(E02==9~1,
                                TRUE~0),
                conjuge=case_when(E02 %in% c(2,3)~1,
                                  TRUE~0),
                pai=case_when(E02==8~1,
                              TRUE~0),
                filho=case_when(E02 %in% c(4:6)~1,
                                TRUE~0)) %>% 
  dplyr::group_by(A01nficha) %>%
  dplyr::mutate(filho=sum(filho),
                neto=sum(neto),
                bisneto=sum(bisneto),
                sogro=sum(sogro),
                conjuge=sum(conjuge),
                pai=sum(pai)) %>%
  dplyr::ungroup() %>% 
  #  # Criar variáveis para análise
  dplyr::mutate(
    # Criar um código de identificação das RAs
    ra=factor(case_when(A01ra==1~"Plano Piloto",
                        A01ra==2~"Gama",
                        A01ra==3~"Taguatinga",
                        A01ra==4~"Brazlândia",
                        A01ra==5~"Sobradinho",
                        A01ra==6~"Planaltina",
                        A01ra==7~"Paranoá",
                        A01ra==8~"Núcleo Bandeirante",
                        A01ra==9~"Ceilândia",
                        A01ra==10~"Guará",
                        A01ra==11~"Cruzeiro",
                        A01ra==12~"Samambaia",
                        A01ra==13~"Santa Maria",
                        A01ra==14~"São Sebastião",
                        A01ra==15~"Recanto das Emas",
                        A01ra==16~"Lago Sul",
                        A01ra==17~"Riacho Fundo",
                        A01ra==18~"Lago Norte",
                        A01ra==19~"Candangolândia",
                        A01ra==20~"Águas Claras",
                        A01ra==21~"Riacho Fundo II",
                        A01ra==22~"Sudoeste/Octogonal",
                        A01ra==23~"Varjão",
                        A01ra==24~"Park Way",
                        A01ra==25~"Scia/Estrutural",
                        A01ra==26~"Sobradinho II",
                        A01ra==27~"Jardim Botânico",
                        A01ra==28~"Itapoã",
                        A01ra==29~"SIA",
                        A01ra==30~"Vicente Pires",
                        A01ra==31~"Fercal")),
    
    
    #grupo da ped
    grupo_ped=factor(case_when(A01ra %in% c(1,27,16,18,24,22)~"Grupo 1",
                               A01ra %in% c(20,19,11,2,10,8,5,26,3,30)~"Grupo 2",
                               A01ra %in% c(4,9,6,17,21,29,12,13,14)~"Grupo 3",
                               A01ra %in% c(31,28,7,15,25,23)~"Grupo 4")),
    # chefe
    chefe=case_when(E02==1~1,
                    TRUE~0), 
    # Criar variável para o sexo mulher
    mulher=case_when(E03==2~1,
                     TRUE~0),

    # Criar variável para deficiencia(visual, auditiva,motora ou mental)
        deficiente=case_when(E06 %in% c(2,3) | E07%in% c(2,3) | E08%in% c(2,3) | E09%in% c(1,2)~1,
                         TRUE~0),
    # Criar variável para tempo gasto com afazeres domésticos
        afazer=case_when(G18==88888~NA_real_,
                     TRUE~G18),
    # Criar variável para maternidade/paternidade
        sit_pat_mat=case_when(E02 %in% c(1:3)&filho==1~1,
                          E02 %in% c(4,5)&neto==1~1,
                          E02==8~1,
                          E02==13&pai==1~1,
                          E02==9&conjuge==1~1,
                          E02==10&bisneto==1~1,
                          TRUE~0),
    # Criar variável para negro 
    negro=case_when(E04 %in% c(2,4)~1,
                    TRUE~0),
    
    # Criar variável para casado
    casado=case_when(E12 %in% c(2,6)~1,
                     TRUE~0),
    # Criar variável para estudante
    estuda=case_when(F02 %in% c(1:2)~1,
                     TRUE~0),
    #Criar variavel para tempo até escola
    tempo=case_when(G11 %in% c(88888,99999)~NA_real_,
                    TRUE~G11),
    # Criar variável para calculo de horas trabalhadas no mês
        hora_trab=case_when(G17 %in% c(88888,99999,0)~NA_real_,
                        TRUE~G17*4.35),
    # Criar variável de escolaridade
    anos_est=case_when(F02 %in% c(1,2)&F07<=3~0,
                       F02 %in% c(1,2)&F07==4&F08==1~0,
                       F02 %in% c(1,2)&F07==4&F08==2~1,
                       F02 %in% c(1,2)&F07==4&F08==3~2,
                       F02 %in% c(1,2)&F07==4&F08==4~3,
                       F02 %in% c(1,2)&F07==4&F08==5~4,
                       F02 %in% c(1,2)&F07==4&F08==6~5,
                       F02 %in% c(1,2)&F07==4&F08==7~6,
                       F02 %in% c(1,2)&F07==4&F08==8~7,
                       F02 %in% c(1,2)&F07==4&F08==9~7,
                       F02 %in% c(1,2)&F07%in%c(5,6)&F08==1~8,
                       F02 %in% c(1,2)&F07%in%c(5,6)&F08==2~9,
                       F02 %in% c(1,2)&F07%in%c(5,6)&F08==3~10,
                       F02 %in% c(1,2)&F07%in%c(5,6)&F08==4~10,
                       F02 %in% c(1,2)&F07==7~4,
                       F02 %in% c(1,2)&F07==8~9,
                       F02 %in% c(1,2)&F07==9&F09==1&F10%in%c(1,2)~15,
                       F02 %in% c(1,2)&F07==9&F09==1&F10==3~17,
                       F02 %in% c(1,2)&F07==9&F09==1&F10==4~21,
                       F02 %in% c(1,2)&F07==9&F09==2~13,
                       F02 %in% c(1,2)&F07==10&F09==1&F10==1~15,
                       F02 %in% c(1,2)&F07==10&F09==1&F10==2~16,
                       F02 %in% c(1,2)&F07==10&F09==1&F10==3~17,
                       F02 %in% c(1,2)&F07==10&F09==1&F10==4~21,
                       F02 %in% c(1,2)&F07==10&F09==2~15,
                       F02 %in% c(1,2)&F07==11&F09==1&F10==1~15,
                       F02 %in% c(1,2)&F07==11&F09==1&F10==2~16,
                       F02 %in% c(1,2)&F07==11&F09==1&F10==3~17,
                       F02 %in% c(1,2)&F07==11&F09==1&F10==4~21,
                       F02 %in% c(1,2)&F07==11&F09==2~16,
                       F02 %in% c(1,2)&F07==12&F09==1&F10==c(1,2,3)~17,
                       F02 %in% c(1,2)&F07==12&F09==1&F10==4~21,
                       F02 %in% c(1,2)&F07==12&F09==2~19,
                       F02==3&F11==1~0,
                       F02==3&F11%in%c(2,3)&F12==1~1,
                       F02==3&F11%in%c(2,3)&F12==2~2,
                       F02==3&F11%in%c(2,3)&F12==3~3,
                       F02==3&F11%in%c(2,3)&F12==4~4,
                       F02==3&F11%in%c(2,3)&F12==5~5,
                       F02==3&F11%in%c(2,3)&F12==6~6,
                       F02==3&F11%in%c(2,3)&F12==7~7,
                       F02==3&F11%in%c(2,3)&F12==8~8,
                       F02==3&F11%in%c(2,3)&F12==9~8,
                       F02==3&F11==4&F12==1~9,
                       F02==3&F11==4&F12==2~10,
                       F02==3&F11==4&F12%in%c(3,4)~11,
                       F02==3&F11==5&F13==1~8,
                       F02==3&F11==5&F13==2~4,
                       F02==3&F11==6&F13==1~11,
                       F02==3&F11==6&F13==2~9,
                       F02==3&F11==7&F13==1~15,
                       F02==3&F11==7&F13==2~13,
                       F02==3&F11==8&F13==1~16,
                       F02==3&F11==8&F13==2~15,
                       F02==3&F11==9&F13==1~17,
                       F02==3&F11==9&F13==2~16,
                       F02==3&F11==10&F13==1~21,
                       F02==3&F11==10&F13==2~19,
                       F02==4~0),
    
    escolaridade_nao_estuda=case_when(F02==4~"Sem escolaridade",
                                      F11==1~"Fundamental incompleto",
                                      F11==2&F12 %in% c(1:7,10)~"Fundamental incompleto",
                                      F11==3&F12 %in% c(1:8,10)~"Fundamental incompleto",
                                      F11==5&F13==2~"Fundamental incompleto",
                                      F11==2&F12==8~"Fundamental completo",
                                      F11==3&F12==9~"Fundamental completo",
                                      F11==5&F13==1~"Fundamental completo",
                                      F11==4&F12 %in% c(1:2,10)~"Médio incompleto",
                                      F11==6&F13==2~"Médio incompleto",
                                      F11==4&F12 %in% c(3,4)~"Médio completo",
                                      F11==6&F13==1~"Médio completo",
                                      F11==7&F13==2~"Superior incompleto",
                                      F11==7&F13==1~"Superior completo",
                                      F11 %in% c(8:10)~"Superior completo",
                                      TRUE~NA_character_),
    
    escolaridadet=case_when(F07 %in% c(3,4,7)~"Fundamental incompleto",
                            F07 %in% c(5,6,8)~"Médio incompleto",
                            F07==9~"Superior incompleto",
                            F07 %in% c(10:12)~"Superior completo",
                            TRUE~escolaridade_nao_estuda),
    escolaridadet=case_when(F09==1&F10 %in% c(1:4)~"Superior completo",
                            TRUE~escolaridadet),
    escolaridade=factor(ordered(case_when(escolaridadet=="Sem escolaridade"~1,
                                          escolaridadet=="Fundamental incompleto"~2,
                                          escolaridadet=="Fundamental completo"~3,
                                          escolaridadet=="Médio incompleto"~4,
                                          escolaridadet=="Médio completo"~5,
                                          escolaridadet=="Superior incompleto"~6,
                                          escolaridadet=="Superior completo"~7),
                                levels=c(1:7),
                                labels=c("Sem escolaridade","Fundamental incompleto","Fundamental completo",
                                         "Médio incompleto","Médio completo","Superior incompleto","Superior completo"))),
    
    # Criar variável para ensino superior
    superior=case_when(escolaridade=="Superior completo"~1,
                       TRUE~0),
    # Criar variável para ensino médio
    ensino_medio=case_when(escolaridade=="Médio completo"~1,
                           TRUE~0),
    #nascido no df
    nascido_df=case_when(E13==1~1,
                         TRUE~0),
    #plano de saude
    plano=case_when(E10 %in% c(1:3)~1,
                    TRUE~0),
    # Criar variável para quem tem trabalho remunerado
    trabalha=case_when(G05 %in% c(1:3)~1,
                       G05 %in% c(88,99)~NA_real_,
                       TRUE~0),
    # Criar variável para quem trabalha no setor público
    # Retirar G05 é para evitar inconsistências na equação de salários
    # e descritivas
    setor_publico=case_when(G13==2~1,
                            G08 %in% c(10,11,16)~NA_real_,
                            TRUE~0),
    #DUVIDA######
    # Criar variável para setor informal (empregado sem carteira, autônomo e ajuda no negocio familiar)
    informal=case_when(G13==3 & G08 %in% c(1,2,4,14,15,16)~1,
                       G08 %in% c(5,6,7,8) & G09==2~1,
                       G13==88~NA_real_,
                       G09==88~NA_real_,
                       TRUE~0),
    #Criar variável para quem trabalha no DF
    trabalha_pp=case_when(G07 ==1~1,
                          A01ra==1 & G07==33~1,
                          TRUE~0),
    
    # Criar variável para renda do trabalho
    renda_trab=case_when(G16 %in% c(77777,88888,99999)~NA_real_,
                         TRUE~as.numeric(G16*infladefla/hora_trab)),
    
    # Criar variável com o log da renda do trabalho
    log_renda_trab=case_when(renda_trab==0~log((renda_trab*infladefla)+1),
                             TRUE~log(renda_trab*infladefla)),
    
    # Renda domicliar sem a renda da pessoa
    renda_individual=ifelse(G16 %in% c(77777,88888),NA,ifelse(G16==99999,0,G16))+
      ifelse(G19 %in% c(77777,88888),NA, ifelse(G19 %in% c(66666,99999),0,G19))+
      ifelse(G201 %in% c(77777,88888),NA, ifelse(G201 %in% c(66666,99999),0,G201))+
      ifelse(G202 %in% c(77777,88888),NA, ifelse(G202 %in% c(66666,99999),0,G202))+
      ifelse(G203 %in% c(77777,88888),NA, ifelse(G203 %in% c(66666,99999),0,G203))+
      ifelse(G204 %in% c(77777,88888),NA, ifelse(G204 %in% c(66666,99999),0,G204)),
    
    renda_dom_sp= (renda_dom-renda_individual)*infladefla,
    
    experiencia=idade_calculada-7-anos_est,
    
    # Criar variável com outras rendas
    trab_sec=case_when(G19 %in% c(66666,77777,88888,99999)~NA_real_,
                       TRUE~as.numeric(G19)),
    aposentadoria=case_when(G201 %in% c(66666,77777,88888,99999)~NA_real_,
                            TRUE~as.numeric(G201)),
    pensao=case_when(G202 %in% c(66666,77777,88888,99999)~NA_real_,
                     TRUE~as.numeric(G202)),
    outros_rendimentos=case_when(G203 %in% c(66666,77777,88888,99999)~NA_real_,
                                 TRUE~as.numeric(G203)),
    beneficios=case_when(G204 %in% c(66666,77777,88888,99999)~NA_real_,
                         TRUE~as.numeric(G204)),
    
    # Criar variável para quem contribui com a previdência pública
    previdencia_pub=case_when(G12==1~1,
                              !G05 %in% c(1:3)~NA_real_,
                              G12 %in% c(88,99)~NA_real_,
                              TRUE~0),
    
    # Criar variável para o ano da pesquisa
    ANO=as.character(year(mdy(datavisita))),
    
    # Criar variável para o mês da pesquisa
    MES=month(mdy(datavisita),label = F, abbr = F),
    
    # Criar um ID único para cada pessoa da base
    ID_pes=1:nrow(.)) 

# Declarar plano amostral 
sample.pdad2018 <- 
  survey::svydesign(id = ~A01nficha,
                    strata = ~A01setor,
                    weights = ~PESO_PRE,
                    nest=TRUE,
                    data=pdad_2018)

# Recuperar pós-estrato
post.pop <- pdad_2018 %>% 
  dplyr::select(A01setor,POP_AJUSTADA_PROJ) %>% 
  dplyr::group_by(A01setor) %>% 
  dplyr::summarise(Freq=first(POP_AJUSTADA_PROJ)) %>%
  dplyr::ungroup() 

# Declarar o objeto de pós-estrato
svy2018_pes <- survey::postStratify(sample.pdad2018,~A01setor,post.pop)

# Criar objeto para calcular os erros por bootstrap (Rao and Wu's(n - 1) bootstrap)
svy2018 <- survey::as.svrepdesign(svy2018_pes, type = "subbootstrap")

# Tibble svy
svy2018 <- srvyr::as_survey(svy2018)

# Ajustar estratos com apenas uma UPA (adjust=centered)
options(survey.lonely.psu = "adjust")

# Calcular o primeiro estágio
reg_ocup <- survey::svyglm(trabalha~idade_calculada+
                             I(idade_calculada^2)+
                             superior+
                             ensino_medio+
                             mulher+
                             sit_pat_mat+
                             casado+
                             renda_dom_sp+
                             negro+
                             grupo_ped+
                             afazer+
                             deficiente+
                             estuda,
                           subset(svy2018,idade_calculada>=14), 
                           family=binomial(link = "probit"))
# Verificar os resultados
summary(reg_ocup)

# Calcular a inversa de mills
imr <- pdad_2018 %>% 
  dplyr::filter(idade_calculada>=14) %>% 
  dplyr::select(E01,trabalha,idade_calculada,superior,ensino_medio,grupo_ped,
                mulher,sit_pat_mat,casado,renda_dom_sp,negro,estuda,deficiente,afazer) %>% 
  na.omit %>% 
  dplyr::mutate(IMR=invMillsRatio(reg_ocup)$IMR1) %>% 
  dplyr::select(E01,IMR)

# Levar a inversa para a base
pdad_2018 <- pdad_2018 %>% 
  dplyr::left_join(imr)


# Declarar plano amostral novamente
sample.pdad2018 <- 
  survey::svydesign(id = ~A01nficha,
                    strata = ~A01setor,
                    weights = ~PESO_PRE,
                    nest=TRUE,
                    data=pdad_2018)

# Declarar o objeto de pós-estrato
svy2018_pes <- survey::postStratify(sample.pdad2018,~A01setor,post.pop)

# Criar objeto para calcular os erros por bootstrap (Rao and Wuus(n - 1) bootstrap)
svy2018 <- survey::as.svrepdesign(svy2018_pes, type = "subbootstrap")

# Tibble svy
svy2018 <- srvyr::as_survey(svy2018)

# Fazer a regressão de salarios
reg_sal <- survey::svyglm(log_renda_trab~
                            idade_calculada+
                            I(idade_calculada^2)+
                            superior+
                            ensino_medio+
                            #sit_pat_mat+
                            negro+
                            estuda+
                            informal+
                            setor_publico+
                            tempo+
                            trabalha_pp+
                            IMR+
                            deficiente+
                            #afazer+
                            mulher,
                          subset(svy2018,idade_calculada>=14))
# Verificar os resultados
summary(reg_sal)



# Tirar as estatísticas descritiva comum a todos
descritivas0 <- svy2018 %>% 
  srvyr::filter(!(renda_trab==0&trabalha==1),idade_calculada>=14) %>% 
  srvyr::group_by(trabalha) %>% 
  srvyr::summarise_at(vars(idade_calculada,superior,ensino_medio,mulher,sit_pat_mat,renda_dom_sp,negro,deficiente,
                           estuda),
                      list(~ srvyr::survey_mean(.,na.rm=T))) %>% 
  tidyr::gather("VAR","valor",-1) %>% 
  dplyr::filter(stringr::str_detect(VAR,"_se$")==T) %>% 
  dplyr::mutate(trabalha=case_when(trabalha==0~"Não trabalha",
                                   trabalha==1~"Trabalha")) %>% 
  tidyr::spread(trabalha,valor) %>% 
  dplyr::mutate(`Não trabalha`=case_when(`Não trabalha`==1~NA_real_,
                                         TRUE~`Não trabalha`))
# Tirar as descritivas dos trabalhadores
descritivas1 <- svy2018 %>% 
  srvyr::filter(trabalha==1) %>% 
  srvyr::summarise_at(vars(informal,trabalha_pp,setor_publico,
                           renda_trab,tempo),
                      list(~ srvyr::survey_mean(.,na.rm=T))) %>% 
  t() %>% 
  data.frame %>% 
  rownames_to_column() %>% 
  dplyr::filter(stringr::str_detect(rowname,"_se")==T) %>% 
  dplyr::rename_all(list(~c("VAR","Trabalha")))

descritivas_ep <- descritivas0 %>% 
  dplyr::bind_rows(descritivas1)

# Tirar as estatísticas descritiva comum a todos
descritivas0 <- svy2018 %>% 
  srvyr::filter(!(renda_trab==0&trabalha==1),idade_calculada>=14) %>% 
  srvyr::group_by(trabalha) %>% 
  srvyr::summarise_at(vars(idade_calculada,superior,ensino_medio,mulher,sit_pat_mat,renda_dom_sp,negro,deficiente,
                           estuda),
                   list(~ srvyr::survey_mean(.,na.rm=T))) %>% 
  tidyr::gather("VAR","valor",-1) %>% 
  dplyr::filter(str_detect(VAR,"_se$")==F) %>% 
  dplyr::mutate(trabalha=case_when(trabalha==0~"Não trabalha",
                                   trabalha==1~"Trabalha")) %>% 
  tidyr::spread(trabalha,valor) %>% 
  dplyr::mutate(`Não trabalha`=case_when(`Não trabalha`==1~NA_real_,
                                         TRUE~`Não trabalha`))
# Tirar as descritivas dos trabalhadores
descritivas1 <- svy2018 %>% 
  srvyr::filter(trabalha==1) %>% 
  srvyr::summarise_at(vars(informal,trabalha_pp,setor_publico,
                           renda_trab,tempo),
                      list(~survey_mean(.,na.rm=T))) %>% 
  t() %>% 
  data.frame %>% 
  rownames_to_column() %>% 
  dplyr::filter(str_detect(rowname,"_se")==F) %>% 
  dplyr::rename_all(list(~c("VAR","Trabalha")))

# Juntar as duas coisas
descritivas <- descritivas0 %>% 
  dplyr::bind_rows(descritivas1) %>% 
  dplyr::mutate(VAR=case_when(VAR=="idade_calculada"~"Idade (média)",
                              VAR=="ensino_medio"~"Ensino médio (%)",
                              VAR=="estuda"~"Estudante (%)",
                              VAR=="sit_pat_mat"~"Tem filho (%)",
                              VAR=="mulher"~"Pessoas do sexo feminino (%)",
                              VAR=="negro"~"Negro (%)",
                              VAR=="renda_dom_sp"~"Renda domiciliar (sem a renda da pessoa) (Média)",
                              VAR=="superior"~"Ensino superior (%)",
                              VAR=="informal"~"Trabalha no setor informal (%)",
                              VAR=="trabalha_pp"~"Trabalha no Plano Piloto (%)",
                              VAR=="setor_publico"~"Trabalha no setor público (%)",
                              VAR=="deficiente"~"Pessoas com alguma deficiência (%)",
                              VAR=="tempo"~"Tempo no emprego principal (média)",
                              VAR=="renda_trab"~"Renda do trabalho (média)")) %>% 
  dplyr::bind_cols(descritivas_ep %>% 
                     dplyr::select(-1)) %>% 
  dplyr::mutate_at(vars(2:5),
                   list(~case_when(VAR %in% c("Ensino médio (%)","Estudante (%)","Pessoas do sexo feminino (%)",
                                              "Negro (%)","Ensino superior (%)","Trabalha no setor informal (%)",
                                              "Pessoas com alguma deficiência (%)","Tem filho (%)",
                                              "Trabalha no Plano Piloto (%)","Trabalha no setor público (%)")~.*100,
                                   TRUE~.)))

