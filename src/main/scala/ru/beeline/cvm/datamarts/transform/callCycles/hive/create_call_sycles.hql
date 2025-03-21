CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_SCHEMA}.{TABLE_NAME}(
                subs_key                                  STRING         COMMENT 'Номер абонента',
                ban_key                                   BIGINT         COMMENT 'Номер договора',
                subscriber_sk                             BIGINT         COMMENT 'Суррогатный ключ',
                first_ctn                                 STRING         COMMENT 'first ctn',
                first_ban                                 BIGINT         COMMENT 'first ban',
                time_key                                  STRING         COMMENT 'Месяц, в котором стартовал цикл звонков (формат PyyyyMM)',
                cycle_start                               DATE           COMMENT 'Дата начала цикла',
                cycle_end                                 DATE           COMMENT 'Дата окончания цикла',
                cycle_duration                            INT            COMMENT 'Длительность цикла (дн)',
                calls_cnt                                 BIGINT         COMMENT 'Количество звонков в цикле',
                calls_duration                            DECIMAL(21,0)  COMMENT 'Общая длительность (sum) звонков в цикле (недозвоны и автоответчик обнуляются)',
                camp_FMC_flg                              INT            COMMENT 'Флаг кампании FMC в цикле',
                cycle_use_case                            STRING         COMMENT 'Основной use_case цикла',
                cycle_result                              STRING         COMMENT 'Общий результат попыток дозвона в цикле',
                flg_3                                     INT            COMMENT 'Флаг контактного клиента',
                flg_2                                     INT            COMMENT 'Флаг условно контактного абонента',
                target                                    INT            COMMENT 'Таргет для текущего цикла: 3 - контактный, 2 - условно контактный, 1 - не контактный, -1 - не было циклов (КГ)',
                A_no_answer_cnt                           BIGINT         COMMENT 'Количество звонков в цикле со статусами Абонент не ответил, Автоответчик',
                B_no_time_cnt                             BIGINT         COMMENT 'Количество звонков в цикле со статусами Не знаком, нет времени',
                C_no_cnt                                  BIGINT         COMMENT 'Количество звонков в цикле со статусами Не будет',
                D_yes_cnt                                 BIGINT         COMMENT 'Количество звонков в цикле со статусами Будет',
                E_refusal_cnt                             BIGINT         COMMENT 'Количество звонков в цикле со статусами Отказ от ответа',
                F_block_cnt                               BIGINT         COMMENT 'Количество звонков в цикле со статусами Блокировка',
                A_no_answer_duration                      BIGINT         COMMENT 'Длительность (sum) звонков в цикле со статусами Абонент не ответил, Автоответчик',
                B_no_time_duration                        DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в цикле со статусами Не знаком, нет времени',
                C_no_duration                             DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в цикле со статусами Не будет',
                D_yes_duration                            DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в цикле со статусами Будет',
                E_refusal_duration                        DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в цикле со статусами Отказ от ответа',
                F_block_duration                          DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в цикле со статусами Блокировка',
                prev_cycle_start                          DATE           COMMENT 'Дата начала предыдущего цикла',
                prev_cycle_end                            DATE           COMMENT 'Дата окончания предыдущего цикла',
                prev_cycle_days                           INT            COMMENT 'Интервал (дн) между текущим циклом и предыдущим ',
                prev_cycle_duration                       INT            COMMENT 'Длительность  предыдущего цикла (дн)',
                prev_cycle_calls_cnt                      BIGINT         COMMENT 'Количество звонков в предыдущем цикле',
                prev_cycle_calls_duration                 DECIMAL(21,0)  COMMENT 'Общая длительность (sum) звонков в предыдущем цикле (недозвоны и автоответчик обнуляются)',
                prev_cycle_use_case                       STRING         COMMENT 'Основной use_case предыдущего цикла',
                prev_cycle_result                         STRING         COMMENT 'Общий результат попыток дозвона в предыдущем цикле ',
                prev_cycle_A_no_answer_cnt                BIGINT         COMMENT 'Количество звонков в предыдущем цикле со статусами Абонент не ответил, Автоответчик',
                prev_cycle_B_no_time_cnt                  BIGINT         COMMENT 'Количество звонков в предыдущем цикле со статусами Не знаком, нет времени',
                prev_cycle_C_no_cnt                       BIGINT         COMMENT 'Количество звонков в предыдущемцикле со статусами Не будет',
                prev_cycle_D_yes_cnt                      BIGINT         COMMENT 'Количество звонков в предыдущем цикле со статусами Будет',
                prev_cycle_E_refusal_cnt                  BIGINT         COMMENT 'Количество звонков в предыдущем цикле со статусами Отказ от ответа',
                prev_cycle_F_block_cnt                    BIGINT         COMMENT 'Количество звонков в предыдущем цикле со статусами Блокировка',
                prev_cycle_A_no_answer_duration           BIGINT         COMMENT 'Длительность (sum) звонков в предыдущем цикле со статусами Абонент не ответил, Автоответчик',
                prev_cycle_B_no_time_duration             DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в предыдущем цикле со статусами Не знаком, нет времени',
                prev_cycle_C_no_duration                  DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в предыдущем цикле со статусами Не будет',
                prev_cycle_D_yes_duration                 DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в предыдущем цикле со статусами Будет',
                prev_cycle_E_refusal_duration             DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в предыдущем цикле со статусами Отказ от ответа',
                prev_cycle_F_block_duration               DECIMAL(21,0)  COMMENT 'Длительность (sum) звонков в предыдущем цикле со статусами Блокировка',
                prev_cycle_target                         INT            COMMENT 'Таргет для прошлого цикла: 3 - контактный, 2 - условно контактный, 1 - не контактный, -1 - не было циклов (КГ)',
                prev_cycles_cnt                           BIGINT         COMMENT 'Количество циклов, прошедших за последние 270 дней от старта текущего цикла',
                prev_cycles_duration                      DOUBLE         COMMENT 'Длительность  предыдущих циклов (дн), прошедших за последние 270 дней от старта текущего цикла',
                prev_cycles_calls_cnt                     BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней)',
                prev_cycles_calls_duration                DOUBLE         COMMENT 'Общая длительность (sum) звонков в циклах (за последние 270 дней)',
                prev_cycles_intervals                     DOUBLE         COMMENT 'Средний интервал между циклами (за последние 270 дней)',
                prev_cycles_A_no_answer_cnt               BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней) со статусами Абонент не ответил, Автоответчик',
                prev_cycles_B_no_time_cnt                 BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней) со статусами Не знаком, нет времени',
                prev_cycles_C_no_cnt                      BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней) со статусами Не будет',
                prev_cycles_D_yes_cnt                     BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней) со статусами Будет',
                prev_cycles_E_refusal_cnt                 BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней) со статусами Отказ от ответа',
                prev_cycles_F_block_cnt                   BIGINT         COMMENT 'Количество звонков в предыдущих циклах (за последние 270 дней) со статусами Блокировка',
                prev_cycles_A_no_answer_duration          DOUBLE         COMMENT 'Длительность (sum) звонков в предыдущих циклах (за последние 270 дней) со статусами Абонент не ответил, Автоответчик',
                prev_cycles_B_no_time_duration            DOUBLE         COMMENT 'Длительность (sum) звонков в предыдущих циклах (за последние 270 дней) со статусами Не знаком, нет времени',
                prev_cycles_C_no_duration                 DOUBLE         COMMENT 'Длительность (sum) звонков в предыдущих циклах (за последние 270 дней) со статусами Не будет',
                prev_cycles_D_yes_duration                DOUBLE         COMMENT 'Длительность (sum) звонков в предыдущих циклах (за последние 270 дней) со статусами Будет',
                prev_cycles_E_refusal_duration            DOUBLE         COMMENT 'Длительность (sum) звонков в предыдущих циклах (за последние 270 дней) со статусами Отказ от ответа',
                prev_cycles_F_block_duration              DOUBLE         COMMENT 'Длительность (sum) звонков в предыдущих циклах (за последние 270 дней) со статусами Блокировка',
                prev_cycles_A_no_answer_pct               DOUBLE         COMMENT 'Доля циклов (за последние 270 дней), закрытых с общим результатом Абонент не ответил или Автоответчик',
                prev_cycles_B_no_time_pct                 DOUBLE         COMMENT 'Доля циклов (за последние 270 дней), закрытых с общим результатом Не знаком, нет времени',
                prev_cycles_C_no_pct                      DOUBLE         COMMENT 'Доля циклов (за последние 270 дней), закрытых с общим результатом Не будет',
                prev_cycles_D_yes_pct                     DOUBLE         COMMENT 'Доля циклов (за последние 270 дней), закрытых с общим результатом Будет',
                prev_cycles_E_refusal_pct                 DOUBLE         COMMENT 'Доля циклов (за последние 270 дней), закрытых с общим результатом Отказ от ответа',
                prev_cycles_F_block_pct                   DOUBLE         COMMENT 'Доля циклов (за последние 270 дней), закрытых с общим результатом Блокировка')
            PARTITIONED BY (time_key_src DATE  COMMENT 'Поле партицирования - первый день месяца, в котором стартовал цикл звонков (yyyy-MM-01)')
            STORED AS ORC;