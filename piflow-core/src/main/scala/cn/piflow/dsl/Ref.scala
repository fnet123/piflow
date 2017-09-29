package cn.piflow.dsl

/**
 * @author bluejoe2008@gmail.com
 */
case class Ref[T](var value: T = null.asInstanceOf[T]) {
	def apply(): T = value;
	def get(): T = value;
	def update(t: T) = value = t;
	def :=(t: T) = {
		value = t;
		t;
	}
}