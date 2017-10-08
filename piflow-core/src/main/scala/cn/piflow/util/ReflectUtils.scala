package cn.piflow.util

/**
	* Created by bluejoe on 2017/9/30.
	*/
object ReflectUtils {
	implicit def reflected(o: Any) = new ReflectedObject(o);
}

class ReflectedObject(o: Any) {
	def doGet(name: String): AnyRef = {
		val field = o.getClass.getDeclaredField(name);
		field.setAccessible(true);
		field.get(o);
	}

	def doCall(name: String)(args: Any*) = {
		val method = o.getClass.getDeclaredMethod(name);
		method.setAccessible(true);
		method.invoke(o, args);
	}
}
