package ru.beeline.cvm.commons.metrics

import ru.beeline.dmp.clientvictoriametrics.commons.entity.Tag

case class ConfigForWorkerVictoriaMetrics(urlVictoriaMetrics: String,
                                          openTsdbPort: Int,
                                          openTsdbPath: String,
                                          prometheusPort: Int,
                                          prometheusPath: String,
                                          product: Tag,
                                          source: Tag,
                                          target: Tag,
                                          process: Tag,
                                          relevanceDate: Tag
                                         )
