package com.jeanie.spark;

import scala.Serializable;

public abstract interface ILogProcessor extends Serializable
{
  public abstract void generate(String paramString1, String paramString2, String paramString3)
    throws Exception;
}
