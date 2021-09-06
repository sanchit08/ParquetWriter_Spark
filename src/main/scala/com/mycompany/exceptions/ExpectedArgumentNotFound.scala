package com.mycompany.exceptions

case class ExpectedArgumentNotFound(message: String = "") extends Exception(message)